#!/usr/bin/env python3
"""
fetch_rakuten_min_prices.py
- hotel_master.json を読み込み、楽天 VacantHotelSearch で
  今日から28日分 × 各ホテルの「その日の最安値（円）」を抽出
- data/competitor_min_prices.json を更新（失敗日は前回値を保持）
- 必要な環境変数: RAKUTEN_APP_ID
"""

import os, json, time, datetime as dt
from pathlib import Path
import requests

APP_ID = os.environ.get("RAKUTEN_APP_ID")
if not APP_ID:
    raise SystemExit("❌ RAKUTEN_APP_ID が未設定です。GitHub Secrets を確認してください。")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_PATH = DATA_DIR / "hotel_master.json"
OUT_PATH = DATA_DIR / "competitor_min_prices.json"

API_URL = "https://app.rakuten.co.jp/services/api/Travel/VacantHotelSearch/20170426"
UA = "ota-bridge/1.0 (+https://github.com/)"

WINDOW_DAYS = 28          # UIでは7日単位で表示、データは28日先まで保持
REQUEST_DELAY = 0.6       # 連続アクセスの間引き（秒）

def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    hotels = [h for h in m.get("hotels", []) if h.get("enabled") and h.get("rakuten_hotel_no")]
    return [{"id": h["id"], "hotel_no": int(h["rakuten_hotel_no"])} for h in hotels]

def date_range(start: dt.date, days: int):
    for i in range(days):
        yield (start + dt.timedelta(days=i))

def ensure_out_skeleton(prev: dict, hotel_ids: list):
    """既存ファイルがなければ 'meta' と 'days' の器を作る"""
    if not prev:
        prev = {
            "meta": {
                "version": 1,
                "currency": "JPY",
                "source": "rakuten_travel",
                "window_days": WINDOW_DAYS,
                "note": "次の28日分を日次更新。UIでは7日単位でページング表示。"
            },
            "last_updated": None,
            "days": {}
        }
    # daysの各日付にホテルキーを用意（nullで初期化はしない、後段で埋める）
    return prev

def extract_min_price_from_response(js: dict):
    """
    VacantHotelSearchのレスポンス内から dailyCharge の候補価格を収集し最小値を返す。
    - 価格候補: dailyCharge.total / dailyCharge.rakutenCharge / dailyCharge.minCharge など
    """
    prices = []

    def walk(o):
        if isinstance(o, dict):
            if "dailyCharge" in o and isinstance(o["dailyCharge"], dict):
                dc = o["dailyCharge"]
                for key in ("total", "rakutenCharge", "minCharge", "maxCharge", "charge"):
                    v = dc.get(key)
                    if isinstance(v, (int, float)) and v > 0:
                        prices.append(int(round(v)))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(js)
    if prices:
        return min(prices)
    return None  # 空室なし等

def fetch_min_price_for(hotel_no: int, ymd: str):
    """指定ホテル×日付の最安値（円）を取得"""
    params = {
        "applicationId": APP_ID,
        "format": "json",
        "hotelNo": hotel_no,
        "checkinDate": ymd,   # 1泊想定
        "adultNum": 1,
        "roomNum": 1
    }
    headers = {"User-Agent": UA, "Accept": "application/json"}
    r = requests.get(API_URL, params=params, headers=headers, timeout=25)
    r.raise_for_status()
    js = r.json()
    return extract_min_price_from_response(js)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    hotels = load_master()
    hotel_ids = [h["id"] for h in hotels]

    # 既存データの読み込み（前回値フォールバックに使用）
    prev = {}
    if OUT_PATH.exists():
        try:
            with OUT_PATH.open("r", encoding="utf-8") as f:
                prev = json.load(f)
        except Exception:
            prev = {}

    out = ensure_out_skeleton(prev, hotel_ids)
    days_obj = out.setdefault("days", {})

    today = dt.date.today()
    for d in date_range(today, WINDOW_DAYS):
        ymd = d.isoformat()
        # その日の入れ物を用意
        day_bucket = days_obj.setdefault(ymd, {})
        for h in hotels:
            hid = h["id"]
            hotel_no = h["hotel_no"]

            # 既に値があるなら再取得せずスキップ（負荷軽減）
            # ※必要に応じて毎回更新したいなら下の if を外す
            # if isinstance(day_bucket.get(hid), int):
            #     continue

            price = None
            try:
                price = fetch_min_price_for(hotel_no, ymd)
                time.sleep(REQUEST_DELAY)
            except Exception:
                # エラー時は前回値を使う（NaN/0は出さない）
                pass

            if price is None:
                # フォールバック（前回値）
                prev_day = (prev.get("days") or {}).get(ymd, {})
                price = prev_day.get(hid)

            # 整数 or None だけを許可
            if isinstance(price, (int, float)):
                day_bucket[hid] = int(price)
            else:
                # None のままにしておく（UI側で欠損扱い）
                day_bucket[hid] = None

    out["last_updated"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ wrote {OUT_PATH.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
