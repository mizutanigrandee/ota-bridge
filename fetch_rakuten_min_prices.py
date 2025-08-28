# -*- coding: utf-8 -*-
"""
Rakuten VacantHotelSearch で 28日ウィンドウの最安値を取得し
data/competitor_min_prices.json を更新する。
- detailClassCode="D" 固定
- 新規ホテルでも各日付に必ずキーを作成（値は None）
- 失敗や在庫無しは None を入れる（NaN/Infinity は入れない）
- last_updated は UTC ISO8601
"""
import json, os, time, math, sys
from datetime import datetime, timedelta, timezone
import requests

APP_ID = os.environ.get("RAKUTEN_APP_ID", "").strip()
BASE_URL = "https://app.rakuten.co.jp/services/api/Travel/VacantHotelSearch/20170426"
DATA_PATH = "data/competitor_min_prices.json"
MASTER_PATH = "data/hotel_master.json"
WINDOW_DAYS = 28
TIMEOUT = 15

def load_json(path, default):
  if not os.path.exists(path):
    return default
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)

def save_json(path, obj):
  tmp = path + ".tmp"
  with open(tmp, "w", encoding="utf-8") as f:
    json.dump(obj, f, ensure_ascii=False, indent=2)
  os.replace(tmp, path)

def jst_today():
  JST = timezone(timedelta(hours=9))
  return datetime.now(JST).date()

def day_keys_from_today(n):
  start = jst_today()
  return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]

def call_rakuten(date_str, hotel_no):
  """在庫があれば税込最安値(int)、無ければ None を返す。軽いリトライ付き。"""
  params = {
    "applicationId": APP_ID,
    "formatVersion": 2,
    "checkinDate": date_str,
    "checkoutDate": (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
    "hotelNo": hotel_no,
    "detailClassCode": "D",
  }
  for i in range(3):
    try:
      r = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
      if r.status_code == 200:
        js = r.json()
        if not js or "hotels" not in js or not js["hotels"]:
          return None
        m = math.inf
        for h in js["hotels"]:
          try:
            hotel_info = h.get("hotel", [{}])[0]
            for p in hotel_info.get("roomInfo", []):
              charge = p.get("roomCharge", {}).get("total", None)
              if isinstance(charge, (int, float)):
                m = min(m, int(round(charge)))
          except Exception:
            continue
        return None if m is math.inf else int(m)
      elif r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5 * (i + 1))
      else:
        return None
    except requests.RequestException:
      time.sleep(1.5 * (i + 1))
  return None

def main():
  if not APP_ID:
    print("ERROR: RAKUTEN_APP_ID is empty", file=sys.stderr)
    sys.exit(1)

  master = load_json(MASTER_PATH, {"hotels": []})
  hotels = [h for h in master.get("hotels", []) if h.get("enabled") and h.get("rakuten_hotel_no")]
  ids = [h["id"] for h in hotels]

  store = load_json(DATA_PATH, {"meta":{"currency":"JPY","source":"rakuten_travel","window_days":WINDOW_DAYS},"days":{}})

  # 28日分の空枠を用意（各ホテルIDのキーを必ず作る）
  days = day_keys_from_today(WINDOW_DAYS)
  for d in days:
    store["days"].setdefault(d, {})
    for hid in ids:
      store["days"][d].setdefault(hid, None)

  # 取得
  for idx, h in enumerate(hotels, 1):
    hid, hno = h["id"], h["rakuten_hotel_no"]
    print(f"[{idx}/{len(hotels)}] {hid} hotel_no={hno}")
    for d in days:
      price = call_rakuten(d, hno)
      store["days"][d][hid] = price if isinstance(price, int) else None
    time.sleep(0.3)  # polite delay

  store["last_updated"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
  save_json(DATA_PATH, store)
  print("OK: competitor_min_prices.json updated")

if __name__ == "__main__":
  main()
