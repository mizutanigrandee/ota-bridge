#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_jalan_reviews.py
- hotel_master.json を読み込み、jalan_hotel_id を持つ施設の
  「口コミ平均」「口コミ件数」を じゃらん施設ページから取得（低頻度スクレイピング）
- data/ota_facility_meta.json へ jalan ブロックをマージ
- 失敗時は前回値を保持（0/NaNは出さない）
"""

import json, re, time, datetime as dt
from pathlib import Path
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_PATH = DATA_DIR / "hotel_master.json"
OUT_PATH = DATA_DIR / "ota_facility_meta.json"

# ブラウザ相当のヘッダ
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.7,en;q=0.6",
    "Referer": "https://www.jalan.net/"
}
TIMEOUT = 25
DELAY_SEC = 3.0  # 1施設あたりの待機

def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    return [h for h in m.get("hotels", []) if h.get("enabled") and h.get("jalan_hotel_id")]

def _try_get(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.text:
            return r.text
        return None
    except Exception:
        return None

def parse_from_json_ld(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    schema.org の aggregateRating を最優先。type 属性のバリエーションや @graph にも対応。
    BeautifulSoup の .string は None になりがちなので .get_text() を使用。
    """
    def to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def to_int(x):
        try:
            return int(str(x).replace(",", ""))
        except Exception:
            return None

    for tag in soup.find_all("script", lambda t: t and "ld+json" in t.lower()):
        raw = (tag.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        candidates = []
        if isinstance(data, dict):
            candidates.append(data)
            if isinstance(data.get("@graph"), list):
                candidates.extend(data["@graph"])
        elif isinstance(data, list):
            candidates.extend(data)

        for d in candidates:
            if not isinstance(d, dict):
                continue
            ar = d.get("aggregateRating")
            if isinstance(ar, dict):
                avg = to_float(ar.get("ratingValue"))
                cnt = to_int(ar.get("reviewCount") or ar.get("ratingCount"))
                if avg is not None or cnt is not None:
                    return avg, cnt
    return None, None

def parse_from_microdata(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    itemtype=AggregateRating の Microdata を拾う（念のためのフォールバック）
    """
    avg = cnt = None
    for div in soup.find_all(attrs={"itemtype": re.compile("AggregateRating")}):
        rv = div.find(attrs={"itemprop": "ratingValue"})
        rc = div.find(attrs={"itemprop": re.compile("reviewCount|ratingCount")})
        if rv and (avg is None):
            try: avg = float(rv.get_text().strip())
            except Exception: pass
        if rc and (cnt is None):
            try: cnt = int(rc.get_text().strip().replace(",", ""))
            except Exception: pass
        if avg is not None or cnt is not None:
            return avg, cnt
    return None, None

def parse_from_text(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    テキストから概算抽出（最終フォールバック）
    - 「総合4.3」「総合評価 4.3」「クチコミ総合4.3」等
    - 「クチコミ 1,234件」「口コミ 1,234件」等
    """
    text = soup.get_text(" ", strip=True)

    # 総合評価
    avg = None
    for pat in [
        r"(?:総合(?:評価)?|クチコミ総合)\s*([0-5](?:\.\d)?)",
        r"([0-5](?:\.\d)?)\s*点",
        r"評価\s*([0-5](?:\.\d)?)",
    ]:
        m = re.search(pat, text)
        if m:
            try:
                val = float(m.group(1))
                if 0.0 <= val <= 5.0:
                    avg = val
                    break
            except Exception:
                pass

    # 口コミ件数（カタカナ/漢字）
    cnt = None
    for word in ["クチコミ", "口コミ"]:
        for m in re.finditer(rf"{word}.{{0,50}}?([0-9,]+)\s*件", text):
            try:
                c = int(m.group(1).replace(",", ""))
                cnt = max(cnt or 0, c)
            except Exception:
                pass
    if cnt is None:
        for m in re.finditer(r"([0-9,]+)\s*件", text):
            try:
                c = int(m.group(1).replace(",", ""))
                cnt = max(cnt or 0, c)
            except Exception:
                pass

    return avg, cnt

def fetch_jalan_review(jalan_hotel_id: str) -> Tuple[Optional[float], Optional[int]]:
    """
    じゃらんの施設ページから口コミ平均/件数を取得（順に試行）
      1) https://www.jalan.net/yadXXXXXX/
      2) https://www.jalan.net/yadXXXXXX/kuchikomi/
      3) https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo=XXXXXX
    """
    urls = [
        f"https://www.jalan.net/{jalan_hotel_id}/",
        f"https://www.jalan.net/{jalan_hotel_id}/kuchikomi/",
    ]
    m = re.search(r"(\d+)", jalan_hotel_id)
    if m:
        yad_no = m.group(1)
        urls.append(f"https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo={yad_no}")

    last_html = None
    for u in urls:
        html = _try_get(u)
        if html:
            last_html = html
            soup = BeautifulSoup(html, "html.parser")

            # 1) JSON-LD
            avg, cnt = parse_from_json_ld(soup)
            if avg is not None or cnt is not None:
                print(f"[jalan] {jalan_hotel_id}: found in JSON-LD at {u}")
                return avg, cnt

            # 2) Microdata
            avg, cnt = parse_from_microdata(soup)
            if avg is not None or cnt is not None:
                print(f"[jalan] {jalan_hotel_id}: found in microdata at {u}")
                return avg, cnt

            # 3) テキスト
            avg, cnt = parse_from_text(soup)
            if avg is not None or cnt is not None:
                print(f"[jalan] {jalan_hotel_id}: found in text at {u}")
                return avg, cnt

        time.sleep(1.0)  # URL切替時の短い待機

    # ここまで見つからなければ None
    if last_html is None:
        print(f"[jalan] {jalan_hotel_id}: failed to fetch any page")
    else:
        print(f"[jalan] {jalan_hotel_id}: fetched but not found rating")
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
        hid = h["id"]
        jid = h.get("jalan_hotel_id")
        if not jid:
            continue

        avg = cnt = None
        try:
            avg, cnt = fetch_jalan_review(jid)
            time.sleep(DELAY_SEC)  # polite
        except Exception as e:
            print(f"[jalan] {jid}: error {e!r}")

        # 前回値フォールバック
        prev = (base.get("hotels") or {}).get(hid, {}).get("jalan", {})
        if avg is None:
            avg = prev.get("review_avg")
        if cnt is None:
            cnt = prev.get("review_count")

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
