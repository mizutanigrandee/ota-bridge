#!/usr/bin/env python
# fetch_ikyu_reviews.py
# 一休/Yahoo!トラベルの口コミ 平均/件数を取得し、data/ota_facility_meta.json に「安全マージ」保存
# 優先: ikyu_url / フォールバック: yahoo_travel_url（値は実務上同等のため ikyu ブロックに格納）

import os, json, re
import datetime as dt
from typing import Dict, Any, Optional, Tuple
from playwright.sync_api import sync_playwright

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
MASTER_PATH = os.path.join(DATA, "hotel_master.json")
META_PATH   = os.path.join(DATA, "ota_facility_meta.json")

# ---- 待機・挙動設定（先方負荷を下げる & 遅延描画対策） ----
WAIT_MS   = 3500   # 初期描画待機 (↑)
DELAY_MS  = 1200   # ホテル間ウェイト
HEADLESS  = True
MAX_RETRY = 3
BASE_WAIT_MS = 800 # リトライバックオフ基準

# ---- ユーティリティ ----
FW_MAP = str.maketrans("０１２３４５６７８９，．（）", "0123456789,.()")

def to_halfwidth(s: str) -> str:
    return s.translate(FW_MAP)

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
        s = to_halfwidth(s).replace(",", "").strip()
        return int(s)
    except Exception:
        return None

def jnum_to_float(s: str) -> Optional[float]:
    try:
        s = to_halfwidth(s).replace(",", "").strip()
        return float(s)
    except Exception:
        return None

# ---- 抽出（優先：JSON-LD、次点：テキスト） ----
def extract_from_ldjson(html: str) -> Optional[Tuple[Optional[float], Optional[int]]]:
    """application/ld+json から ratingValue と reviewCount/ratingCount を取得"""
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S | re.I
    ):
        block_txt = m.group(1)
        try:
            block = json.loads(block_txt)
        except Exception:
            continue

        blocks = block if isinstance(block, list) else [block]
        for b in blocks:
            if not isinstance(b, dict):
                continue
            agg = b.get("aggregateRating")
            if not isinstance(agg, dict):
                continue

            rv = agg.get("ratingValue")
            rc = agg.get("reviewCount")
            if rc is None:
                rc = agg.get("ratingCount")  # ← Yahoo対策

            rating = jnum_to_float(str(rv)) if rv is not None else None
            count  = jnum_to_int(str(rc))   if rc is not None else None

            # バリデーション（評価は 0〜5 のみ有効）
            if rating is not None and not (0 <= rating <= 5):
                rating = None

            if rating is not None or count is not None:
                return (rating, count)
    return None

def extract_from_text(html: str) -> Optional[Tuple[Optional[float], Optional[int]]]:
    """
    テキストから堅牢に抽出。
    - 評価は 0〜5 の小数のみ（年号・IDを除外）
    - 件数は「◯◯件」のみ
    - 「総合得点」「口コミ」「クチコミ」「レビュー」「評価」近傍を優先
    """
    text = re.sub(r"<[^>]+>", " ", html)
    text = to_halfwidth(text)
    text = re.sub(r"\s+", " ", text)

    anchors = ["総合得点", "口コミ", "クチコミ", "レビュー", "評価"]
    near_chunks = []
    for kw in anchors:
        for m in re.finditer(re.escape(kw), text):
            i = m.start()
            near_chunks.append(text[max(0, i-220): i+260])
    if not near_chunks:
        near_chunks = [text]

    # 代表パターン：総合得点 4.04 （25件）
    pat_rating1 = re.compile(r"(?:総合得点|評価)\s*[:：]?\s*([0-5](?:\.\d{1,2})?)")
    pat_rating2 = re.compile(r"([0-5](?:\.\d{1,2})?)\s*(?:点|/5|／5|5点)")
    pat_count   = re.compile(r"\(\s*(\d{1,3}(?:,\d{3})*)\s*件\s*\)|(\d{1,3}(?:,\d{3})*)\s*件")

    def find_rating(chunk: str) -> Optional[float]:
        for pat in (pat_rating1, pat_rating2):
            m = pat.search(chunk)
            if m:
                v = jnum_to_float(m.group(1))
                if v is not None and 0 <= v <= 5:
                    return v
        return None

    def find_count(chunk: str) -> Optional[int]:
        m = pat_count.search(chunk)
        if m:
            g = m.group(1) or m.group(2)
            return jnum_to_int(g)
        return None

    rating = None
    count  = None
    for ch in near_chunks:
        if rating is None:
            rating = find_rating(ch)
        if count is None:
            count = find_count(ch)
        if rating is not None or count is not None:
            break

    # 最後の保険：全文から
    if rating is None:
        rating = find_rating(text)
    if count is None:
        count = find_count(text)

    if rating is not None and not (0 <= rating <= 5):
        rating = None

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
            # 遅延描画があるため "networkidle" を使用
            page.goto(url, wait_until="networkidle", timeout=45000)
            page.wait_for_timeout(WAIT_MS)
            return page.content()
        except Exception as e:
            w = BASE_WAIT_MS * (2 ** i)
            print(f"⚠️ goto失敗({i+1}/{MAX_RETRY}) {e} → {w}ms待機して再試行")
            page.wait_for_timeout(w)
    return None

def pick_target_url(h: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    優先: ikyu_url / フォールバック: yahoo_travel_url
    戻り値: (url, source_label) 例: ("https://www.ikyu.com/...", "ikyu")
    """
    ikyu = (h.get("ikyu_url") or "").strip()
    if ikyu:
        return ikyu, "ikyu"
    yahoo = (h.get("yahoo_travel_url") or "").strip()
    if yahoo:
        return yahoo, "yahoo"
    return None

def main():
    master = load_json(MASTER_PATH)
    if not master or "hotels" not in master:
        raise SystemExit("❌ data/hotel_master.json が読めません")

    # 対象抽出（enabled かつ ikyu_url or yahoo_travel_url のどちらかがある）
    targets = []
    for h in master["hotels"]:
        if not h.get("enabled"):
            continue
        picked = pick_target_url(h)
        if picked:
            url, src = picked
            targets.append({"id": h["id"], "url": url, "src": src})

    if not targets:
        print("ℹ️ 対象ホテル（ikyu_url/yahoo_travel_urlあり）がありません。処理を終了します。")
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
            url, hid, src = t["url"], t["id"], t["src"]
            try:
                html = try_get_content(page, url)
                if not html:
                    print(f"⚠️ {hid}: 取得できず（リトライ尽き） from={src} {url}")
                else:
                    pair = extract_from_ldjson(html) or extract_from_text(html)
                    if pair:
                        rating, count = pair
                        r = float(rating) if isinstance(rating, (int, float)) else None
                        c = int(count)    if isinstance(count, int) else None
                        if r is not None or c is not None:
                            results[hid] = {"review_avg": r, "review_count": c}
                            print(f"✅ {hid}: rating={r} count={c} from={src} {url}")
                        else:
                            print(f"⚠️ {hid}: 数値が取得できず from={src} {url}")
                    else:
                        print(f"⚠️ {hid}: パターン不一致で抽出不可 from={src} {url}")
            except Exception as e:
                print(f"⚠️ {hid}: エラー {e} from={src} {url}")
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
