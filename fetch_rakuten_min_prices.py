#!/usr/bin/env python
# fetch_rakuten_min_prices.py
# 目的：
# - data/hotel_master.json を読み、enabled かつ rakuten_hotel_no を持つホテル一覧を取得
# - 84日ぶん（JST）の日付キーを必ず作成し、各ホテルIDを必ず埋める（初期値は null）
# - 既存 JSON があれば meta を引き継ぎ（不足キーは補完）、days は今回84日で再構築
# - 楽天API取得は後段で上書き（失敗しても null のまま＝OK）

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

# ===== 収集期間・レート制御（必要に応じて調整可） =====
WINDOW_DAYS       = 84        # ← 28 から 84（約12週）へ拡張
THROTTLE_PER_DAY  = 0.30      # 各日ループの間隔（秒）
THROTTLE_PER_HOTEL= 0.25      # 各ホテルAPI呼び出しの間隔（秒）
HTTP_TIMEOUT_SEC  = 20

def iso_utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def jst_today() -> dt.date:
    # UTC+9 を足す（依存を増やさない）
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
    # 84日×全ホテルIDのキーを必ず用意（値は null）
    days: Dict[str, Dict[str, Any]] = {}
    for d in dates:
        row = {}
        for hid in enabled_ids:
            row[hid] = None
        days[d] = row
    return days

# --- 最小の楽天取得（成功すれば上書き、失敗しても無視） ---
# 窓口：VacantHotelSearch 20170426（detailClassCode=D / 施設番号直接指定）
RAKUTEN_ENDPOINT = "https://app.rakuten.co.jp/services/api/Travel/VacantHotelSearch/20170426"

def fetch_min_price_for_date(hotels: List[Dict[str, Any]], ymd: str) -> Dict[str, int]:
    """
    指定日の最安値を hotelNo（施設番号）でピンポイント取得。
    - 各ホテルごとに 1 リクエスト
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
            "hotelNo": hotel_no,   # 施設を直接指定
            "carrier": 0,
            "responseType": "large",
            "hits": 10,
            "adultNum": 1
        }
        try:
            resp = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=HTTP_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            time.sleep(THROTTLE_PER_HOTEL)
            continue

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

        time.sleep(THROTTLE_PER_HOTEL)  # マナーウェイト

    return results


def main():
    hotels = read_enabled_hotels()  # [{'id':..., 'rakuten_hotel_no':...}, ...]
    enabled_ids = [h["id"] for h in hotels]
    dates = date_range_jst(WINDOW_DAYS)

    # 既存ファイル（meta があれば引き継ぎ、なければデフォルト）
    old = load_json(OUT_PATH) or {}
    meta = {
        "currency": "JPY",
        "source": "rakuten_travel",
        "window_days": WINDOW_DAYS,
    }
    if isinstance(old, dict) and isinstance(old.get("meta"), dict):
        # 既存メタを尊重しつつ必須キーを補完
        tmp = dict(old["meta"])
        tmp.setdefault("currency", "JPY")
        tmp.setdefault("source", "rakuten_travel")
        tmp.setdefault("window_days", WINDOW_DAYS)
        meta = tmp

    # 本仕様で保証したいメタ情報を上書き/補完
    meta.update({
        "person": 1,                    # 1名利用で取得
        "pricing_basis": "dailyCharge"  # その日のプランの最小値を採用
    })

    # まず「空の84日×全ホテルID」を作る（差分仕様担保）
    days = build_empty_days(enabled_ids, dates)

    # --- 取得で上書き（失敗しても null のまま） ---
    for ymd in dates:
        time.sleep(THROTTLE_PER_DAY)  # polite wait（楽天APIにやさしく）
        found = fetch_min_price_for_date(hotels, ymd)
        if found:
            for hid, price in found.items():
                days.setdefault(ymd, {}).setdefault(hid, None)
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
