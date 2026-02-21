# fetch_rakuten_reviews.py
# --- Fetch + Safe merge writer for ota_facility_meta.json --------------------
import os, json, time, datetime as dt
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
META_PATH = os.path.join(DATA_DIR, "ota_facility_meta.json")
MASTER_PATH = os.path.join(DATA_DIR, "hotel_master.json")

RAKUTEN_APP_ID = os.environ.get("RAKUTEN_APP_ID", "").strip()
RAKUTEN_ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "").strip()  # あれば新APIで使う

ENDPOINT_NEW = "https://openapi.rakuten.co.jp/engine/api/Travel/HotelDetailSearch/20170426"
ENDPOINT_OLD = "https://app.rakuten.co.jp/services/api/Travel/HotelDetailSearch/20170426"

THROTTLE_SEC = 0.35
MAX_RETRIES = 5
TIMEOUT_SEC = 20

def _load_meta():
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"hotels": {}, "last_updated": None}

def _safe_int(x):
    # None/NaN/空文字/ "1,234" を考慮
    try:
        if x is None:
            return None
        s = str(x).replace(",", "").strip()
        if s == "":
            return None
        n = int(float(s))
        return n if n >= 0 else None
    except Exception:
        return None

def _safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        n = float(s)
        return round(n, 2)
    except Exception:
        return None

def save_rakuten_reviews(rk_results_by_id):
    """
    rk_results_by_id: { hotel_id: {"review_avg": 4.27, "review_count": 1776} } だけを更新
    - 既存の jalan ブロックはそのまま保持
    - 取得失敗のIDは 上書きしない（前回値を温存）
    """
    meta = _load_meta()
    meta.setdefault("hotels", {})

    updated = 0
    for hid, rk in rk_results_by_id.items():
        if rk is None:
            continue
        avg = _safe_float(rk.get("review_avg"))
        cnt = _safe_int(rk.get("review_count"))
        if avg is None or cnt is None:
            continue

        entry = meta["hotels"].get(hid, {})
        jalan_block = entry.get("jalan")  # jalan は保持
        entry["rakuten"] = {"review_avg": avg, "review_count": cnt}
        if jalan_block is not None:
            entry["jalan"] = jalan_block
        meta["hotels"][hid] = entry
        updated += 1

    meta["last_updated"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[rakuten reviews] merged: {updated} hotels; wrote {META_PATH}")

def _load_enabled_hotels_rakuten_no():
    """
    data/hotel_master.json から enabled なホテルだけ拾い、
    { hotel_id: rakuten_hotel_no } を返す
    """
    try:
        with open(MASTER_PATH, "r", encoding="utf-8") as f:
            master = json.load(f)
    except Exception:
        master = {}

    out = {}
    for h in master.get("hotels", []):
        if not h.get("enabled"):
            continue
        hid = h.get("id")
        hotel_no = h.get("rakuten_hotel_no")
        if not hid or hotel_no is None:
            continue
        try:
            out[hid] = int(hotel_no)
        except Exception:
            continue
    return out

def _extract_review_avg_cnt(payload):
    """
    HotelDetailSearchのレスポンスから reviewAverage / reviewCount を抽出
    形式ブレを吸収して 'hotelBasicInfo' を探す方式
    """
    try:
        hotels = payload.get("hotels") or []
        for item in hotels:
            if not isinstance(item, dict):
                continue
            hotel_arr = item.get("hotel")
            if not isinstance(hotel_arr, list):
                continue
            for part in hotel_arr:
                if isinstance(part, dict) and "hotelBasicInfo" in part and isinstance(part["hotelBasicInfo"], dict):
                    basic = part["hotelBasicInfo"]
                    avg = _safe_float(basic.get("reviewAverage"))
                    cnt = _safe_int(basic.get("reviewCount"))
                    return avg, cnt
    except Exception:
        pass
    return None, None

def _request_with_retry(url, headers, params):
    backoff = 1.0
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SEC)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return None

def fetch_rakuten_review(hotel_no):
    """
    accessKeyがあれば新API（openapi + Authorization Bearer）、
    なければ旧APIへフォールバック（まず更新を復活させる）
    """
    if not RAKUTEN_APP_ID:
        raise SystemExit("❌ Secrets に RAKUTEN_APP_ID が必要です")

    params = {
        "applicationId": RAKUTEN_APP_ID,
        "format": "json",
        "formatVersion": 2,
        "responseType": "middle",
        "hotelNo": hotel_no,
    }

    if RAKUTEN_ACCESS_KEY:
        headers = {"Authorization": f"Bearer {RAKUTEN_ACCESS_KEY}"}
        return _request_with_retry(ENDPOINT_NEW, headers, params)

    # フォールバック（旧API）
    headers = {}
    return _request_with_retry(ENDPOINT_OLD, headers, params)

def main():
    hotels = _load_enabled_hotels_rakuten_no()
    if not hotels:
        raise SystemExit("❌ hotel_master.json に enabled & rakuten_hotel_no のホテルが見つかりません")

    print(f"[rakuten reviews] accessKey={'YES' if bool(RAKUTEN_ACCESS_KEY) else 'NO'}")
    print(f"[rakuten reviews] endpoint={'NEW' if bool(RAKUTEN_ACCESS_KEY) else 'OLD'}")

    results = {}
    for hid, hotel_no in hotels.items():
        payload = fetch_rakuten_review(hotel_no)
        if isinstance(payload, dict):
            avg, cnt = _extract_review_avg_cnt(payload)
            if avg is not None and cnt is not None:
                results[hid] = {"review_avg": avg, "review_count": cnt}
            else:
                results[hid] = None
        else:
            results[hid] = None

        time.sleep(THROTTLE_SEC)

    save_rakuten_reviews(results)

if __name__ == "__main__":
    main()
# ---------------------------------------------------------------------------
