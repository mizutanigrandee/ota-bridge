#!/usr/bin/env python3
# fetch_rakuten_min_prices.py
# 目的：
# - data/hotel_master.json を読み、enabled かつ rakuten_hotel_no を持つホテル一覧を取得
# - 84日ぶん（JST）の日付キーを必ず作成し、各ホテルIDを必ず埋める（初期値は null）
# - 既存 JSON があれば meta を引き継ぎ（不足キーは補完）、days は今回84日で再構築
# - 楽天API取得は後段で上書き（失敗しても null のまま＝OK）
#
# 新API対応：
# - openapi.rakuten.co.jp を利用
# - applicationId + accessKey 必須
# - 403対策として Origin/Referer/User-Agent を付与

import os
import json
import time
import datetime as dt
from typing import Dict, List, Any, Optional

import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")

MASTER_PATH = os.path.join(DATA_DIR, "hotel_master.json")
OUT_PATH    = os.path.join(DATA_DIR, "competitor_min_prices.json")
LAST_PATH   = os.path.join(DATA_DIR, "last_updated.json")

APP_ID = os.environ.get("RAKUTEN_APP_ID", "").strip()
if not APP_ID:
    raise SystemExit("❌ RAKUTEN_APP_ID が未設定です（GitHub Secrets に設定してください）")

ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "").strip()
if not ACCESS_KEY:
    raise SystemExit("❌ RAKUTEN_ACCESS_KEY が未設定です（GitHub Secrets に設定してください）")

# ===== 収集期間・レート制御（必要に応じて調整可） =====
WINDOW_DAYS        = 84        # 約12週
THROTTLE_PER_DAY   = 0.30      # 各日ループの間隔（秒）
THROTTLE_PER_HOTEL = 0.25      # 各ホテルAPI呼び出しの間隔（秒）
HTTP_TIMEOUT_SEC   = 20
MAX_RETRIES        = 5

# --- 新API（openapi） ---
RAKUTEN_ENDPOINT = "https://openapi.rakuten.co.jp/engine/api/Travel/VacantHotelSearch/20170426"

# 403対策（Rakuten Developers の「許可されたWebサイト」に合わせる）
ALLOWED_ORIGIN  = "https://mizutanigrandee.github.io"
ALLOWED_REFERER = "https://mizutanigrandee.github.io/ota-bridge/"

RAKUTEN_HEADERS = {
    "Authorization": f"Bearer {ACCESS_KEY}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Accept": "application/json",
    "Origin": ALLOWED_ORIGIN,
    "Referer": ALLOWED_REFERER,
}

# デバッグしたい時だけ 1 にする（普段は 0 推奨）
DEBUG = os.environ.get("DEBUG_RAKUTEN", "0") == "1"


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

    rows: List[Dict[str, Any]] = []
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


def _request_json_with_retry(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    - 429 は待ってリトライ
    - 200 以外は None（上位で無視）
    """
    backoff = 1.0
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(
                RAKUTEN_ENDPOINT,
                headers=RAKUTEN_HEADERS,
                params=params,
                timeout=HTTP_TIMEOUT_SEC
            )

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if (ra and ra.isdigit()) else backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 30)
                continue

            if r.status_code != 200:
                if DEBUG:
                    # できるだけ情報を出す（Secretsは出ない）
                    try:
                        j = r.json()
                    except Exception:
                        j = {"text": r.text[:200]}
                    print(f"[rakuten min_prices] HTTP {r.status_code}: {j}")
                return None

            return r.json()

        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    return None


def _extract_min_daily_charge(data: Dict[str, Any], target_hotel_no: int) -> Optional[int]:
    """
    レスポンスから「その日付の dailyCharge 最小値」を拾う
    """
    cand_daily: Optional[int] = None

    items = data.get("hotels", []) or []
    for item in items:
        parts = item.get("hotel", []) or []

        got_hotel_no = None

        for part in parts:
            # basic
            basic = part.get("hotelBasicInfo")
            if isinstance(basic, dict):
                got_hotel_no = basic.get("hotelNo", got_hotel_no)

            # rooms
            room_list = part.get("roomInfo")
            if isinstance(room_list, list):
                for r in room_list:
                    dc = r.get("dailyCharge", {})
                    if not isinstance(dc, dict):
                        continue

                    v = dc.get("rakutenCharge")
                    if v is None:
                        v = dc.get("total")

                    if isinstance(v, (int, float)) and v >= 0:
                        v = int(v)
                        cand_daily = v if cand_daily is None else min(cand_daily, v)

        if got_hotel_no == target_hotel_no and isinstance(cand_daily, int):
            return cand_daily

    return None


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
            "accessKey": ACCESS_KEY,     # ★新API必須
            "format": "json",
            "formatVersion": 2,          # ★安定化（レビュー側と揃える）
            "checkinDate": ymd,
            "checkoutDate": checkout,
            "hotelNo": hotel_no,         # 施設を直接指定
            "carrier": 0,
            "responseType": "large",
            "hits": 10,
            "adultNum": 1
        }

        data = _request_json_with_retry(params)
        if not isinstance(data, dict):
            time.sleep(THROTTLE_PER_HOTEL)
            continue

        price = _extract_min_daily_charge(data, hotel_no)
        if isinstance(price, int) and price >= 0:
            results[hid] = price

        time.sleep(THROTTLE_PER_HOTEL)

    return results


def main():
    hotels = read_enabled_hotels()
    enabled_ids = [h["id"] for h in hotels]
    dates = date_range_jst(WINDOW_DAYS)

    # 既存ファイル（metaがあれば引き継ぎ）
    old = load_json(OUT_PATH) or {}
    meta = {
        "currency": "JPY",
        "source": "rakuten_travel",
        "window_days": WINDOW_DAYS,
    }
    if isinstance(old, dict) and isinstance(old.get("meta"), dict):
        tmp = dict(old["meta"])
        tmp.setdefault("currency", "JPY")
        tmp.setdefault("source", "rakuten_travel")
        tmp.setdefault("window_days", WINDOW_DAYS)
        meta = tmp

    # 本仕様で保証したいメタ情報
    meta.update({
        "person": 1,
        "pricing_basis": "dailyCharge",
        "api": "openapi",
    })

    # まず「空の84日×全ホテルID」を作る
    days = build_empty_days(enabled_ids, dates)

    # 取得で上書き（失敗しても null のまま）
    for ymd in dates:
        time.sleep(THROTTLE_PER_DAY)
        found = fetch_min_price_for_date(hotels, ymd)
        if found:
            for hid, price in found.items():
                days.setdefault(ymd, {}).setdefault(hid, None)
                if isinstance(price, int) and price >= 0:
                    days[ymd][hid] = price

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
