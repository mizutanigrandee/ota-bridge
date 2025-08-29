# scripts/health_check.py
# 最小の健康診断：data/*.json を軽く点検して data/health_report.json を出力（失敗でも exit 0）
import json, os, datetime as dt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def iso_now_utc():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def main():
    report = {"timestamp_utc": iso_now_utc(), "summary": {}, "checks": []}

    def add(status, message, extra=None):
        report["checks"].append({"status": status, "message": message, "extra": extra or {}})

    # 1) master
    enabled_ids = []
    try:
        master = load(os.path.join(DATA, "hotel_master.json"))
        hotels = master.get("hotels", [])
        enabled_ids = [h["id"] for h in hotels if h.get("enabled") and h.get("rakuten_hotel_no")]
        add("OK", f"hotel_master.json 読込OK / enabled={len(enabled_ids)}")
    except Exception as e:
        add("ERROR", f"hotel_master.json 読込失敗: {e}")

    # 2) prices
    try:
        cmpj = load(os.path.join(DATA, "competitor_min_prices.json"))
        meta = cmpj.get("meta", {})
        days = cmpj.get("days", {})
        w = meta.get("window_days")
        if w != 28:
            add("WARN", f"window_days != 28: {w}")
        day_keys = sorted(days.keys())
        miss_keys = 0
        bad_vals = 0
        null_count = 0
        total_cells = 0
        for d in day_keys:
            row = days.get(d, {})
            for hid in enabled_ids:
                total_cells += 1
                if hid not in row:
                    miss_keys += 1
                    continue
                v = row[hid]
                if v is None:
                    null_count += 1
                elif not (isinstance(v, int) and v >= 0):
                    bad_vals += 1
        add("OK", f"competitor_min_prices.json days={len(day_keys)} / cells={total_cells}",
            {"missing_keys": miss_keys, "bad_values": bad_vals,
             "null_ratio": (null_count/total_cells if total_cells else 0)} )
    except Exception as e:
        add("ERROR", f"competitor_min_prices.json 読込失敗: {e}")

    # 3) last_updated
    try:
        lu = load(os.path.join(DATA, "last_updated.json"))
        iso = lu.get("last_updated") or lu.get("lastUpdated") or lu.get("updated_at")
        if not iso:
            add("WARN", "last_updated.json: ISOキーが見つかりません（last_updated推奨）")
        else:
            t = dt.datetime.fromisoformat(iso.replace("Z","+00:00"))
            diff_h = (dt.datetime.now(dt.timezone.utc) - t).total_seconds()/3600
            status = "OK" if diff_h <= 26 else "WARN"
            add(status, f"last_updated ≈ {diff_h:.2f}h")
    except Exception as e:
        add("WARN", f"last_updated.json 読込で警告: {e}")

    # 出力
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "health_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
