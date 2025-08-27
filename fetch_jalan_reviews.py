#!/usr/bin/env python3
"""
fetch_jalan_reviews.py
- hotel_master.json を読み込み、jalan_hotel_id を持つ施設の
  口コミ「総合評価」「口コミ件数」を施設ページから取得（低頻度スクレイピング）
- data/ota_facility_meta.json を更新（既存の楽天データに jalan をマージ）
- 失敗時は前回値を保持（0/NaNは出さない）
- 取得間隔は polite に 3 秒
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

# ブラウザっぽいヘッダ（弾かれにくい）
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
DELAY_SEC = 3.0  # polite

def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    hotels = m.get("hotels", [])
    return [h for h in hotels if h.get("enabled") and h.get("jalan_hotel_id")]

def _try_get(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.text and "robots" not in r.url:
            return r.text
        return None
    except Exception:
        return None

def parse_from_json_ld(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    schema.org の aggregateRating を優先的に探す。
    @graph 配下にも対応。
    """
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        # 候補を列挙（dict, list, @graph のどれでも）
        candidates = []
        if isinstance(data, list):
            candidates.extend(data)
        elif isinstance(data, dict):
            candidates.append(data)
            if isinstance(data.get("@graph"), list):
                candidates.extend(data["@graph"])

        for d in candidates:
            if not isinstance(d, dict):
                continue
            ar = d.get("aggregateRating")
            if isinstance(ar, dict):
                rv = ar.get("ratingValue")
                rc = ar.get("reviewCount") or ar.get("ratingCount")
                try:
                    avg = float(rv) if rv is not None else None
                except Exception:
                    avg = None
                try:
                    cnt = int(str(rc).replace(",", "")) if rc is not None else None
                except Exception:
                    cnt = None
                if avg is not None or cnt is not None:
                    return avg, cnt
    return None, None

def parse_from_text(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    テキストから概算で抽出（フォールバック）
    - 「総合4.3」「総合評価 4.3」など
    - 「クチコミ 1,234件」「口コミ 1,234件」両表記に対応
    """
    text = soup.get_text(" ", strip=True)

    # 総合評価 (0.0~5.0)
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

    # 口コミ件数（カタカナ/漢字どちらも）
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
    じゃらんの施設ページから 口コミ平均/件数 を取得
    優先: https://www.jalan.net/<yadXXXXXX>/
    次点: https://www.jalan.net/<yadXXXXXX>/kuchikomi/
    最後: https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo=<digits>
    """
    # 1) 施設トップ
    urls = [f"https://www.jalan.net/{jalan_hotel_id}/",
            f"https://www.jalan.net/{jalan_hotel_id}/kuchikomi/"]

    # 2) フォールバック（yadNo= 数値）
    m = re.search(r"(\d+)", jalan_hotel_id)
    if m:
        yad_no = m.group(1)
        urls.append(f"https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo={yad_no}")

    html = None
    for u in urls:
        html = _try_get(u)
        if html:
            break

    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD優先
    avg, cnt = parse_from_json_ld(soup)
    if avg is not None or cnt is not None:
        return avg, cnt

    # テキスト抽出フォールバック
    return parse_from_text(soup)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 既存の出力（楽天を含む）を読み込む
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
        jalan_id = h.get("jalan_hotel_id")
        if not jalan_id:
            continue

        avg, cnt = None, None
        try:
            avg, cnt = fetch_jalan_review(jalan_id)
            time.sleep(DELAY_SEC)  # polite
        except Exception:
            pass

        # 失敗時は前回値フォールバック
        prev = (base.get("hotels") or {}).get(hid, {}).get("jalan", {})
        if avg is None:
            avg = prev.get("review_avg")
        if cnt is None:
            cnt = prev.get("review_count")

        entry = base.setdefault("hotels", {}).setdefault(hid, {})
        jalan_block = entry.setdefault("jalan", {})
        jalan_block["review_avg"] = avg
        jalan_block["review_count"] = cnt

    base["date"] = dt.date.today().isoformat()
    base["last_updated"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(base, f, ensure_ascii=False, indent=2)

    print(f"✅ wrote {OUT_PATH.relative_to(ROOT)} (jalan)")

if __name__ == "__main__":
    main()
