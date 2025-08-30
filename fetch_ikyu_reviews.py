#!/usr/bin/env python
# fetch_ikyu_reviews.py
# 一休の口コミ 平均/件数を取得し、data/ota_facility_meta.json に「安全マージ」保存

import os, json, re
import datetime as dt
from typing import Dict, Any, Optional, Tuple
from playwright.sync_api import sync_playwright

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
MASTER_PATH = os.path.join(DATA, "hotel_master.json")
META_PATH   = os.path.join(DATA, "ota_facility_meta.json")

# ---- 待機・挙動設定（先方負荷を下げる） ----
WAIT_MS   = 2500   # 初期描画待機
DELAY_MS  = 1200   # ホテル間ウェイト
HEADLESS  = True
MAX_RETRY = 3
BASE_WAIT_MS = 800 # リトライバックオフ基準

# ---- ユーティリティ ----
def iso_utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def load_json(path: str) -> Any:
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def dump_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def jnum_to_int(s: str) -> Optional[int]:
    try:
        s = s.replace(",", "").strip()
        return int(s)
    except Exception:
        return None

def jnum_to_float(s: str) -> Optional[float]:
    try:
        s = s.replace(",", "").strip()
        return float(s)
    except Exception:
        return None

# ---- 抽出（優先：JSON-LD、次点：テキスト） ----
def extract_from_ldjson(html: str) -> Optional[Tuple[Optional[float], Optional[int]]]:
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S | re.I
    ):
        try:
            block = json.loads(m.group(1))
            blocks = block if isinstance(block, list) else [block]
            for b in blocks:
                agg = b.get("aggregateRating") if isinstance(b, dict) else None
                if isinstance(agg, dict):
                    rv = agg.get("ratingValue")
                    rc = agg.get("reviewCount")
                    rating = jnum_to_float(str(rv)) if rv is not None else None
                    count  = jnum_to_int(str(rc))   if rc is not None else None
                    if rating is not None or count is not None:
                        return (rating, count)
        except Exception:
            continue
    return None

def extract_from_text(html: str) -> Optional[Tuple[Optional[float], Optional[int]]]:
    text = re.sub(r"<[^>]+>", " ", html)
    anchors = ["口コミ", "クチコミ", "レビュー"]
    for kw in anchors:
        idx = text.find(kw)
        if idx != -1:
            win = text[max(0, idx-120): idx+200]
            m_rating = re.search(r"(\d+(?:\.\d+)?)", win)
            m_count  = re.search(r"(\d{1,3}(?:,\d{3})*)\s*件", win)
            rating = jnum_to_float(m_rating.group(1)) if m_rating else None
            count  = jnum_to_int(m_count.group(1)) if m_count  else None
            if rating is not None or count is not None:
                return (rating, count)
    # 最後の保険（誤検出の恐れあり）
    m_count  = re.search(r"(\d{1,3}(?:,\d{3})*)\s*件", text)
    m_rating = re.search(r"(\d+(?:\.\d+)?)", text)
    rating = jnum_to_float(m_rating.group(1)) if m_rating else None
    count  = jnum_to_int(m_count.group(1)) if m_count  else None
    return (rating, count) if (rating is not None or count is not None) else None

def safe_merge(meta: Dict[str, Any], updates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    out = meta.copy() if isinstance(meta, dict) else {}
    hotels = out.setdefault("hotels", {})
    for hid, v in updates.items():
        hotels.setdefault(hid, {})
        hotels[hid]["ikyu"] = {
            "review_avg":   v.get("review_avg"),
            "review_count": v.get("review_count"),
        }
    out["last_updated"] = iso_utc_now()
    return out

# ---- 取得（小リトライ付き） ----
def try_get_content(page, url: str) -> Optional[str]:
    for i in range(MAX_RETRY):
        try:
            page.goto(url, wait_until="load", timeout=45000)
            page.wait_for_timeout(WAIT_MS)
            return page.content()
        except Exception as e:
            w = BASE_WAIT_MS * (2 ** i)
            print(f"⚠️ goto失敗({i+1}/{MAX_RETRY}) {e} → {w}ms待機して再試行")
            page.wait_for_timeout(w)
    return None

def main():
    master = load_json(MASTER_PATH)
    if not master or "hotels" not in master:
        raise SystemExit("❌ data/hotel_master.json が読めません")

    # 対象抽出（enabled & ikyu_urlあり）
    targets = []
    for h in master["hotels"]:
        url = (h.get("ikyu_url") or "").strip()
        if h.get("enabled") and url:
            targets.append({"id": h["id"], "url": url})

    if not targets:
        print("ℹ️ 対象ホテル（ikyu_urlあり）がありません。処理を終了します。")
        return

    results: Dict[str, Dict[str, Any]] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(
            locale="ja-JP",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0 Safari/537.36")
        )
        page = ctx.new_page()

        for t in targets:
            url, hid = t["url"], t["id"]
            try:
                html = try_get_content(page, url)
                if not html:
                    print(f"⚠️ {hid}: 取得できず（リトライ尽き）")
                else:
                    pair = extract_from_ldjson(html) or extract_from_text(html)
                    if pair:
                        rating, count = pair
                        r = float(rating) if isinstance(rating, (int, float)) else None
                        c = int(count) if isinstance(count, int) else None
                        if r is not None or c is not None:
                            results[hid] = {"review_avg": r, "review_count": c}
                            print(f"✅ {hid}: ikyu rating={r} count={c}")
                    else:
                        print(f"⚠️ {hid}: パターン不一致で抽出不可")
            except Exception as e:
                print(f"⚠️ {hid}: エラー {e}")
            finally:
                page.wait_for_timeout(DELAY_MS)

        ctx.close()
        browser.close()

    if not results:
        print("ℹ️ 取得結果が空（スキップ）")
        return

    meta = load_json(META_PATH) or {}
    merged = safe_merge(meta, results)
    dump_json(META_PATH, merged)
    print(f"📝 wrote {META_PATH}  (updated_hotels={len(results)})")

if __name__ == "__main__":
    main()
