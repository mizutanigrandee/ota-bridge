#!/usr/bin/env python
# fetch_rakuten_min_prices.py
# 目的：
# - data/hotel_master.json を読み、enabled かつ rakuten_hotel_no を持つホテル一覧を取得
# - 28日ぶん（JST）の日付キーを必ず作成し、各ホテルIDを必ず埋める（初期値は null）
# - 既存 JSON があれば meta を引き継ぐ（days は今回28日で再構築）
# - 楽天API取得は後段で上書き（失敗しても null のまま＝OK）
#
# ポイント：まず「キーが必ず存在する」状態を担保するのが目的
# 値取得は後段で改善していけば良い（段階実装）

import os
import json
import time
import datetime as dt
from typing import Dict, List, Any

import requests  # requirements に含まれている前提

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")

MASTER_PATH = os.path.join(DATA_DIR, "hotel_master.json")
OUT_PATH    = os.path.join(DATA_DIR, "competitor_min_prices.json")
LAST_PATH   = os.path.join(DATA_DIR, "last_updated.json")

APP_ID = os.environ.get("RAKUTEN_APP_ID", "")
if not APP_ID:
    raise SystemExit("❌ RAKUTEN_APP_ID が未設定です（GitHub Secrets に設定してください）")

WINDOW_DAYS = 28

def iso_utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def jst_today() -> dt.date:
    # UTC+9 を雑に足す（依存を増やさない）
    return (dt.datetime.utcnow() + dt.timedelta(hours=9)).date()

def date_range_jst(days: int) -> List[str]:
    base = jst_today()
    return [(base + dt.timedelta(days=i)).isoformat() for i in range(days)]

def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def read_enabled_hotels() -> List[Dict[str, Any]]:
    master = load_json(MASTER_PATH)
    if not master or "hotels" not in master:
        raise SystemExit("❌ hotel_master.json が読めません")
    rows = []
    for h in master["hotels"]:
        if h.get("enabled") and h.get("rakuten_hotel_no"):
            rows.append({"id": h["id"], "rakuten_hotel_no": int(h["rakuten_hotel_no"])})
    if not rows:
        raise SystemExit("❌ enabled かつ rakuten_hotel_no を持つホテルがゼロでした")
    return rows

def build_empty_days(enabled_ids: List[str], dates: List[str]) -> Dict[str, Dict[str, Any]]:
    # 28日×全ホテルIDのキーを必ず用意（値は null）
    days: Dict[str, Dict[str, Any]] = {}
    for d in dates:
        row = {}
        for hid in enabled_ids:
            row[hid] = None
        days[d] = row
    return days

# --- ここから先は「最小の楽天取得（成功すれば上書き、失敗しても無視）」 ---
# 検索窓口：VacantHotelSearch 20170426
# 仕様は現行運用と同等（detailClassCode="D" 固定）。在庫ゼロや取得失敗は None のままでOK。
RAKUTEN_ENDPOINT = "https://app.rakuten.co.jp/services/api/Travel/VacantHotelSearch/20170426"

def fetch_min_price_for_date(hotels: List[Dict[str, Any]], ymd: str) -> Dict[str, int]:
    """
    指定日の最安値を hotelNo（施設番号）でピンポイント取得。
    - 各ホテルごとに 1 リクエスト（最大でも施設数ぶん）
    - 見つからなければそのホテルは None のまま（上位で null）
    """
    results: Dict[str, int] = {}
    checkout = (dt.date.fromisoformat(ymd) + dt.timedelta(days=1)).isoformat()

    for h in hotels:
        hid = h["id"]
        hotel_no = h["rakuten_hotel_no"]

        params = {
            "applicationId": APP_ID,
            "format": "json",
            "checkinDate": ymd,
            "checkoutDate": checkout,
            "hotelNo": hotel_no,   # ← 施設を直接指定
            "carrier": 0,
            "responseType": "large",
            "hits": 10,
            "adultNum": 1
        }
        try:
            resp = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            time.sleep(0.2)
            continue

        # レスポンスから価格候補を拾う
        # レスポンスから「その日付の dailyCharge 最小値」だけを拾う
        try:
            items = data.get("hotels", []) or []
            for item in items:
                parts = item.get("hotel", []) or []

                got_hotel_no = None
                cand_daily = None   # その日の最小料金

                for part in parts:
                    basic = part.get("hotelBasicInfo")
                    if basic:
                        got_hotel_no = basic.get("hotelNo", got_hotel_no)

                    room_list = part.get("roomInfo")
                    if isinstance(room_list, list):
                        for r in room_list:
                            dc = r.get("dailyCharge", {})
                            v = dc.get("rakutenCharge")
                            if v is None:
                                v = dc.get("total")
                            if isinstance(v, (int, float)) and v >= 0:
                                v = int(v)
                                cand_daily = v if cand_daily is None else min(cand_daily, v)

                # hotelNo が一致していて cand_daily が見つかったら登録
                if got_hotel_no == hotel_no and isinstance(cand_daily, int):
                    results[hid] = cand_daily
                    break

        except Exception:
            pass


        time.sleep(0.25)  # マナーウェイト

    return results




    # hotel情報から最低料金を拾う
    hotel_map = {h["rakuten_hotel_no"]: h["id"] for h in hotels}
    try:
        for item in data.get("hotels", []):
            info = item.get("hotel", [{}])[-1]  # 末尾に price 情報がいることが多い
            basic = info.get("hotelBasicInfo", {})
            hotel_no = basic.get("hotelNo")
            if hotel_no not in hotel_map:
                continue
            # 最低料金の推定（hotelBasicInfo の minCharge を優先。無ければ rooms の料金から最小を推定）
            price = None
            if "minCharge" in basic and isinstance(basic["minCharge"], (int, float)):
                price = int(basic["minCharge"])
            # minCharge が無いときは plans を探索（あれば）
            if price is None:
                try:
                    # hotel の他要素に price 情報が入っていれば拾う（保険）
                    for k in ("hotelRatingInfo", "roomInfo", "hotelFacilitiesInfo"):
                        _ = info.get(k)
                    # ここは環境差が大きいので、一旦スキップ。minCharge 優先。
                except Exception:
                    pass
            if price is not None and price >= 0:
                results[hotel_map[hotel_no]] = int(price)
    except Exception:
        pass
    return results

def main():
    hotels = read_enabled_hotels()  # [{'id':..., 'rakuten_hotel_no':...}, ...]
    enabled_ids = [h["id"] for h in hotels]
    dates = date_range_jst(WINDOW_DAYS)

    # 既存ファイル（meta だけ尊重）
    old = load_json(OUT_PATH) or {}
    meta = old.get("meta", {"currency": "JPY", "source": "rakuten_travel", "window_days": WINDOW_DAYS})
    meta["window_days"] = WINDOW_DAYS  # 強制

    # まず「空の28日×全ホテルID」を作る（ここが今回の重要修正点）
    days = build_empty_days(enabled_ids, dates)

    # --- 取得で上書き（失敗しても null のまま） ---
    for ymd in dates:
        # 最低限の polite wait
        time.sleep(0.3)
        found = fetch_min_price_for_date(hotels, ymd)
        if found:
            for hid, price in found.items():
                # 念のため setdefault（空のキーが必ずある想定だが安全側）
                days.setdefault(ymd, {}).setdefault(hid, None)
                # 0未満は弾く（安全）
                if isinstance(price, int) and price >= 0:
                    days[ymd][hid] = price

    # 出力組み立て
    out = {
        "meta": meta,
        "days": days,
        "last_updated": iso_utc_now(),
    }
    dump_json(OUT_PATH, out)
    dump_json(LAST_PATH, {"last_updated": out["last_updated"]})
    print(f"✅ wrote {OUT_PATH} & {LAST_PATH}  (hotels={len(enabled_ids)}, days={len(dates)})")

if __name__ == "__main__":
    main()
