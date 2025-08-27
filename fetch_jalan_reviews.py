#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_jalan_reviews.py (Playwright対応)
- hotel_master.json を読み込み、jalan_hotel_id の施設ページを取得
- まず requests で静的HTMLを解析 → 見つからなければ Playwright で描画後のDOMを取得して解析
- 口コミ「平均」「件数」を data/ota_facility_meta.json に jalan ブロックとしてマージ
- 失敗時は前回値を保持（0/NaNは出さない）
"""

import json, re, time, datetime as dt
from pathlib import Path
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---- Playwright（描画後DOM取得用） ----
try:
    from playwright.sync_api import sync_playwright
    HAS_PW = True
except Exception:
    HAS_PW = False

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_PATH = DATA_DIR / "hotel_master.json"
OUT_PATH = DATA_DIR / "ota_facility_meta.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.7,en;q=0.6",
    "Referer": "https://www.jalan.net/"
}
REQ_TIMEOUT = 25
DELAY_SEC = 3.0  # polite: 1施設ごと

def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    return [h for h in m.get("hotels", []) if h.get("enabled") and h.get("jalan_hotel_id")]

# ---------- HTML取得 ----------
def _get_requests(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return None

def _get_playwright(url: str) -> Optional[str]:
    if not HAS_PW:
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=UA,
                                      locale="ja-JP",
                                      extra_http_headers={"Accept-Language":"ja-JP,ja;q=0.9"})
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            # 口コミブロックが描画されるのを軽く待つ（あってもなくてもOK）
            page.wait_for_timeout(1200)
            html = page.content()
            ctx.close(); browser.close()
            return html
    except Exception:
        return None

# ---------- 解析 ----------
def _json_ld_avg_cnt(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    def to_float(x):
        try: return float(x)
        except: return None
    def to_int(x):
        try: return int(str(x).replace(",", ""))
        except: return None

    for tag in soup.find_all("script", lambda t: t and "ld+json" in t.lower()):
        raw = (tag.get_text() or "").strip()
        if not raw: continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = []
        if isinstance(data, dict):
            candidates.append(data)
            if isinstance(data.get("@graph"), list):
                candidates += data["@graph"]
        elif isinstance(data, list):
            candidates += data
        for d in candidates:
            if not isinstance(d, dict): continue
            ar = d.get("aggregateRating")
            if isinstance(ar, dict):
                avg = to_float(ar.get("ratingValue"))
                cnt = to_int(ar.get("reviewCount") or ar.get("ratingCount"))
                if avg is not None or cnt is not None:
                    return avg, cnt
    return None, None

def _microdata_avg_cnt(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    avg = cnt = None
    for div in soup.find_all(attrs={"itemtype": re.compile("AggregateRating")}):
        rv = div.find(attrs={"itemprop": "ratingValue"})
        rc = div.find(attrs={"itemprop": re.compile("reviewCount|ratingCount")})
        if rv and avg is None:
            try: avg = float(rv.get_text().strip())
            except: pass
        if rc and cnt is None:
            try: cnt = int(rc.get_text().strip().replace(",", ""))
            except: pass
        if avg is not None or cnt is not None:
            return avg, cnt
    return None, None

def _text_avg_cnt(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    text = soup.get_text(" ", strip=True)
    avg = None
    for pat in [
        r"(?:総合(?:評価)?|クチコミ総合)\s*([0-5](?:\.\d)?)",
        r"([0-5](?:\.\d)?)\s*点",
        r"評価\s*([0-5](?:\.\d)?)",
    ]:
        m = re.search(pat, text)
        if m:
            try:
                v = float(m.group(1))
                if 0.0 <= v <= 5.0: avg = v; break
            except: pass
    cnt = None
    for word in ["クチコミ", "口コミ"]:
        for m in re.finditer(rf"{word}.{{0,50}}?([0-9,]+)\s*件", text):
            try:
                c = int(m.group(1).replace(",", ""))
                cnt = max(cnt or 0, c)
            except: pass
    if cnt is None:
        for m in re.finditer(r"([0-9,]+)\s*件", text):
            try:
                c = int(m.group(1).replace(",", ""))
                cnt = max(cnt or 0, c)
            except: pass
    return avg, cnt

def _extract_avg_cnt(html: str) -> Tuple[Optional[float], Optional[int]]:
    soup = BeautifulSoup(html, "html.parser")
    for fn in (_json_ld_avg_cnt, _microdata_avg_cnt, _text_avg_cnt):
        a, c = fn(soup)
        if a is not None or c is not None:
            return a, c
    return None, None

def fetch_jalan_review(jid: str) -> Tuple[Optional[float], Optional[int]]:
    urls = [
        f"https://www.jalan.net/{jid}/",
        f"https://www.jalan.net/{jid}/kuchikomi/",
    ]
    m = re.search(r"(\d+)", jid)
    if m:
        yad_no = m.group(1)
        urls.append(f"https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo={yad_no}")

    # 1) 静的HTML → 2) Playwright の順で試す
    for u in urls:
        html = _get_requests(u)
        if html:
            a, c = _extract_avg_cnt(html)
            if a is not None or c is not None:
                print(f"[jalan] {jid}: static OK at {u}")
                return a, c
        if HAS_PW:
            html = _get_playwright(u)
            if html:
                a, c = _extract_avg_cnt(html)
                if a is not None or c is not None:
                    print(f"[jalan] {jid}: pw OK at {u}")
                    return a, c
        time.sleep(1.0)
    print(f"[jalan] {jid}: not found (all methods)")
    return None, None

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    base = {"date": dt.date.today().isoformat(), "last_updated": None, "hotels": {}}
    if OUT_PATH.exists():
        try:
            with OUT_PATH.open("r", encoding="utf-8") as f:
                base = json.load(f)
        except Exception:
            pass

    hotels = load_master()
    for h in hotels:
        hid, jid = h["id"], h.get("jalan_hotel_id")
        if not jid: continue

        avg = cnt = None
        try:
            avg, cnt = fetch_jalan_review(jid)
            time.sleep(DELAY_SEC)
        except Exception as e:
            print(f"[jalan] {jid}: error {e!r}")

        prev = (base.get("hotels") or {}).get(hid, {}).get("jalan", {})
        if avg is None: avg = prev.get("review_avg")
        if cnt is None: cnt = prev.get("review_count")

        entry = base.setdefault("hotels", {}).setdefault(hid, {})
        entry.setdefault("jalan", {})
        entry["jalan"]["review_avg"] = avg
        entry["jalan"]["review_count"] = cnt

    base["date"] = dt.date.today().isoformat()
    base["last_updated"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(base, f, ensure_ascii=False, indent=2)
    print(f"✅ wrote {OUT_PATH.relative_to(ROOT)} (jalan)")

if __name__ == "__main__":
    main()
