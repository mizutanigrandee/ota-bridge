# fetch_rakuten_reviews.py
# --- Fetch + Safe merge writer for ota_facility_meta.json --------------------
import os
import json
import time
import datetime as dt
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
META_PATH = os.path.join(DATA_DIR, "ota_facility_meta.json")
MASTER_PATH = os.path.join(DATA_DIR, "hotel_master.json")

RAKUTEN_APP_ID = os.environ.get("RAKUTEN_APP_ID", "").strip()
RAKUTEN_ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "").strip()

ENDPOINT_NEW = "https://openapi.rakuten.co.jp/engine/api/Travel/HotelDetailSearch/20170426"

THROTTLE_SEC = 0.35
MAX_RETRIES = 5
TIMEOUT_SEC = 20


def _jst_today_iso():
    return (dt.datetime.utcnow() + dt.timedelta(hours=9)).date().isoformat()


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _safe_int(x):
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


def _load_enabled_hotels_rakuten_no():
    """
    data/hotel_master.json から enabled なホテルだけ拾い、
    { hotel_id: rakuten_hotel_no } を返す
    """
    master = _load_json(MASTER_PATH, {"hotels": []})
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


def _request_with_retry(url, headers, params):
    """
    - 200: JSON返却（_http_status=200 を付与）
    - 200以外: エラーJSON（error/error_description）を返す（_http_status付き）
    - 429: リトライ
    """
    backoff = 1.0
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SEC)

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 30)
                continue

            # JSONをできるだけ返す
            try:
                payload = r.json()
            except Exception:
                payload = {"error": "http_error", "error_description": r.text[:300]}

            # ステータスを付与
            if isinstance(payload, dict):
                payload["_http_status"] = r.status_code
            else:
                payload = {"_http_status": r.status_code, "_payload": payload}

            return payload

        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    return {"error": "retry_exhausted", "error_description": f"failed after {MAX_RETRIES} retries", "_http_status": -1}


def fetch_rakuten_detail(hotel_no):
    """
    新API（openapi + accessKey必須）で HotelDetailSearch を叩く
    """
    if not RAKUTEN_APP_ID:
        raise SystemExit("❌ Secrets に RAKUTEN_APP_ID が必要です")
    if not RAKUTEN_ACCESS_KEY:
        raise SystemExit("❌ Secrets に RAKUTEN_ACCESS_KEY が必要です（新API必須）")

    params = {
        "applicationId": RAKUTEN_APP_ID,
        "accessKey": RAKUTEN_ACCESS_KEY,   # ★パラメータでも渡す
        "format": "json",
        "formatVersion": 2,
        "responseType": "middle",
        "hotelNo": hotel_no,
    }
    headers = {
    "Authorization": f"Bearer {RAKUTEN_ACCESS_KEY}",
    "User-Agent": "Mozilla/5.0 (compatible; ota-bridge/1.0; +https://github.com/mizutanigrandee/ota-bridge)",
    "Accept": "application/json",
}

    return _request_with_retry(ENDPOINT_NEW, headers, params)


def _find_review_fields(obj):
    """
    レスポンス構造に依存せず、どこかにある reviewAverage / reviewCount を再帰探索で拾う
    """
    if isinstance(obj, dict):
        if "reviewAverage" in obj and "reviewCount" in obj:
            return obj.get("reviewAverage"), obj.get("reviewCount")
        for v in obj.values():
            ra, rc = _find_review_fields(v)
            if ra is not None and rc is not None:
                return ra, rc
    elif isinstance(obj, list):
        for v in obj:
            ra, rc = _find_review_fields(v)
            if ra is not None and rc is not None:
                return ra, rc
    return None, None


def save_rakuten_reviews(rk_results_by_id):
    """
    rk_results_by_id: { hotel_id: {"review_avg": 4.27, "review_count": 1776} } だけを更新
    - 既存の jalan ブロックは保持
    - 取得失敗のIDは上書きしない（前回値温存）
    """
    meta = _load_json(META_PATH, {"hotels": {}, "last_updated": None})
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

    meta["date"] = _jst_today_iso()
    meta["last_updated"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    _save_json(META_PATH, meta)

    print(f"[rakuten reviews] merged: {updated} hotels; wrote {META_PATH}")


def main():
    hotels = _load_enabled_hotels_rakuten_no()
    if not hotels:
        raise SystemExit("❌ hotel_master.json に enabled & rakuten_hotel_no のホテルが見つかりません")

    print(f"[rakuten reviews] accessKey={'YES' if bool(RAKUTEN_ACCESS_KEY) else 'NO'}")
    print("[rakuten reviews] endpoint=NEW")

    results = {}
    first_debug_printed = False

    for hid, hotel_no in hotels.items():
        payload = fetch_rakuten_detail(hotel_no)

        # 1件だけ構造デバッグ（キーだけ）※秘密は含まれない
        if not first_debug_printed and isinstance(payload, dict):
            keys = list(payload.keys())[:30]
            print(f"[debug] first payload keys: {keys}")
            first_debug_printed = True

        status = payload.get("_http_status") if isinstance(payload, dict) else None

        # HTTPエラーが見えたらログ
        if isinstance(payload, dict) and status and status != 200:
            print(
                f"[rakuten reviews] API error hotelNo={hotel_no}: "
                f"status={status} error={payload.get('error')} desc={payload.get('error_description')}"
            )
            results[hid] = None
            time.sleep(THROTTLE_SEC)
            continue

        # reviewAverage/reviewCount を再帰探索で拾う
        ra_raw, rc_raw = _find_review_fields(payload)
        avg = _safe_float(ra_raw)
        cnt = _safe_int(rc_raw)

        if avg is not None and cnt is not None:
            results[hid] = {"review_avg": avg, "review_count": cnt}
        else:
            # ここに来るなら「構造は返ってるが、レビュー項目が見つからない」ケース
            print(f"[rakuten reviews] WARN no review fields found hotelNo={hotel_no}")
            results[hid] = None

        time.sleep(THROTTLE_SEC)

    save_rakuten_reviews(results)


if __name__ == "__main__":
    main()
# ---------------------------------------------------------------------------
