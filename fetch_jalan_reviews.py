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

UA = "ota-bridge/1.0 (+https://github.com/) JalanReviewFetcher"
TIMEOUT = 20
DELAY_SEC = 3.0


def load_master():
    with MASTER_PATH.open("r", encoding="utf-8") as f:
        m = json.load(f)
    hotels = m.get("hotels", [])
    return [h for h in hotels if h.get("enabled") and h.get("jalan_hotel_id")]


def _try_get(url: str) -> Optional[str]:
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return None


def parse_from_json_ld(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    schema.org の aggregateRating を優先的に探す
    """
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        # 配列/辞書どちらにも対応
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            ar = d.get("aggregateRating") if isinstance(d, dict) else None
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
                if avg or cnt:
                    return avg, cnt
    return None, None


def parse_from_text(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
    """
    テキストから概算で抽出（フォールバック）
    例: 「総合 4.3」「口コミ 1,234件」など
    """
    text = soup.get_text(" ", strip=True)

    # 総合評価 (0.0~5.0)
    avg = None
    for pat in [
        r"総合(?:評価)?\s*([0-5](?:\.\d)?)",
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

    # 口コミ件数（最大に近い値を採用）
    cnt = None
    # 「口コミ」周辺 50 文字にある「◯件」を拾う
    for m in re.finditer(r"口コミ.{0,50}?([0-9,]+)\s*件", text):
        try:
            c = int(m.group(1).replace(",", ""))
            cnt = max(cnt or 0, c)
        except Exception:
            pass
    # それでも取れなければ全体から件数らしきものを拾う
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
    URL 優先: https://www.jalan.net/<yadXXXXXX>/
    フォールバック: https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo=<digits>
    """
    # 1) 通常の施設直URL
    html = _try_get(f"https://www.jalan.net/{jalan_hotel_id}/")

    # 2) フォールバック（数字部分のみで yadNo クエリに）
    if not html:
        m = re.search(r"(\d+)", jalan_hotel_id)
        if m:
            yad_no = m.group(1)
            html = _try_get(f"https://www.jalan.net/uw/uwp3200/uww3201.do?yadNo={yad_no}")

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

    # 既存の楽天出力（あれば）を読み込む
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

        # 既存エントリにマージ（楽天があっても壊さない）
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
