#!/usr/bin/env python3
"""
fetch_rakuten_reviews.py
- hotel_master.json を読み込み、楽天のホテルNoごとに SimpleHotelSearch を叩いて
  reviewAverage / reviewCount を取得
- data/ota_facility_meta.json に出力（MVP：楽天のみ）
- 環境変数 RAKUTEN_APP_ID が必須
"""

import os, json, time, datetime as dt
from pathlib import Path
import requests

APP_ID = os.environ.get("RAKUTEN_APP_ID")
if not APP_ID:
    raise SystemExit("❌ RAKUTEN_APP_ID が未設定です（GitHub Secrets に登録してください）")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_PATH = DATA_DIR / "hotel_master.json"
OUT_PATH = DATA_DIR / "ota_facility_meta.json"

API_URL = "https://app.rakuten.co.jp/services/api/Travel/SimpleHotelSearch/20170426"
UA = "ota-bridge/1.0 (+https://github.com/)"

def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    hotels = m.get("hotels", [])
    # enabled かつ rakuten_hotel_no があるものだけ
    return [h for h in hotels if h.get("enabled") and h.get("rakuten_hotel_no")]

def fetch_rakuten_review(hotel_no: int):
    """楽天 SimpleHotelSearch から reviewAverage / reviewCount を取得"""
    params = {
        "applicationId": APP_ID,
        "hotelNo": hotel_no,
        "format": "json"
    }
    headers = {"User-Agent": UA, "Accept": "application/json"}
    r = requests.get(API_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    js = r.json()

    # 期待構造: {"hotels": [{"hotel": [{"hotelBasicInfo": {...}}]}]}
    hotels = js.get("hotels") or []
    if not hotels:
        return None, None  # 見つからない場合は None を返す

    hb = hotels[0].get("hotel", [{}])[0].get("hotelBasicInfo", {})
    avg = hb.get("reviewAverage")
    cnt = hb.get("reviewCount")
    # 数値化（無い場合は None）
    try:
        avg = float(avg) if avg is not None else None
    except Exception:
        avg = None
    try:
        cnt = int(cnt) if cnt is not None else None
    except Exception:
        cnt = None
    return avg, cnt

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    hotels = load_master()

    # 既存ファイルがあれば読み込み（失敗時フォールバックに使う）
    prev = {}
    if OUT_PATH.exists():
        try:
            with OUT_PATH.open("r", encoding="utf-8") as f:
                prev = json.load(f)
        except Exception:
            prev = {}

    out = {
        "date": dt.date.today().isoformat(),
        "last_updated": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hotels": {}
    }

    for h in hotels:
        hid = h["id"]
        hotel_no = h["rakuten_hotel_no"]

        avg, cnt = None, None
        try:
            avg, cnt = fetch_rakuten_review(int(hotel_no))
            # マナー：連続アクセスを少し間引く
            time.sleep(0.6)
        except Exception as e:
            # 失敗時は前回値を保持（NaN/0 を流さない）
            pass

        # 前回値フォールバック
        if avg is None or cnt is None:
            prev_h = (prev.get("hotels") or {}).get(hid, {}).get("rakuten", {})
            if avg is None:
                avg = prev_h.get("review_avg")
            if cnt is None:
                cnt = prev_h.get("review_count")

        out["hotels"][hid] = {
            "rakuten": {
                "review_avg": avg,
                "review_count": cnt
            }
        }

    # 上書き保存
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ wrote {OUT_PATH.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
