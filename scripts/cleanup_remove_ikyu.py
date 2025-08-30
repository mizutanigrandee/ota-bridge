#!/usr/bin/env python
# scripts/cleanup_remove_ikyu.py
# hotel_master.json から ikyu_url / yahoo_travel_url / ikyu_hotel_id を削除
# ota_facility_meta.json から "ikyu" ブロックを削除

import os, json, datetime as dt

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(REPO_ROOT)  # ../ でリポジトリ直下へ

MASTER = os.path.join(REPO_ROOT, "data", "hotel_master.json")
META   = os.path.join(REPO_ROOT, "data", "ota_facility_meta.json")

def load(path):
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def dump(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def utc_now():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def clean_master():
    data = load(MASTER)
    if not data or "hotels" not in data: 
        print("skip: hotel_master.json not found or malformed")
        return 0
    removed_keys = 0
    for h in data["hotels"]:
        for k in ("ikyu_url", "yahoo_travel_url", "ikyu_hotel_id"):
            if k in h:
                del h[k]
                removed_keys += 1
    if removed_keys:
        data.setdefault("meta", {})["last_updated"] = utc_now()
        dump(MASTER, data)
    print(f"master: removed {removed_keys} key(s)")
    return removed_keys

def clean_meta():
    data = load(META)
    if not data or "hotels" not in data:
        print("skip: ota_facility_meta.json not found or malformed")
        return 0
    removed_hotels = 0
    for hid, blocks in list(data["hotels"].items()):
        if isinstance(blocks, dict) and "ikyu" in blocks:
            del blocks["ikyu"]
            removed_hotels += 1
    if removed_hotels:
        data["last_updated"] = utc_now()
        dump(META, data)
    print(f"meta: removed ikyu block from {removed_hotels} hotel(s)")
    return removed_hotels

def main():
    a = clean_master()
    b = clean_meta()
    if not (a or b):
        print("no changes")

if __name__ == "__main__":
    main()
