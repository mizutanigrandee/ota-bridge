# --- Safe merge writer for ota_facility_meta.json ----------------------------
import os, json, datetime as dt

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
META_PATH = os.path.join(DATA_DIR, "ota_facility_meta.json")

def _load_meta():
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"hotels": {}, "last_updated": None}

def _safe_int(x):
    # None/NaN/空文字は None に
    try:
        n = int(x)
        return n if n >= 0 else None
    except Exception:
        return None

def _safe_float(x):
    try:
        n = float(x)
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
            # 取得失敗などはスキップ（上書きしない）
            continue
        avg  = _safe_float(rk.get("review_avg"))
        cnt  = _safe_int(rk.get("review_count"))
        if avg is None or cnt is None:
            # どちらか欠けたら安全のためスキップ
            continue

        entry = meta["hotels"].get(hid, {})
        # ★ jalan は触らない
        jalan_block = entry.get("jalan")
        # ★ rakuten だけ更新
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
# -----------------------------------------------------------------------------
