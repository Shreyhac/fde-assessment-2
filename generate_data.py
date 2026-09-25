"""Generate fixed-seed synthetic cafeteria sources plus a source manifest.

Usage: python3 generate_data.py [raw_dir]   (default: data/raw)
"""
import csv, hashlib, json, random, sqlite3, sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
RAW = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)
random.seed(10252)

shift_config = {
    "source_type": "synthetic JSON configuration",

    "service_target_minutes": 12,
    "shifts": {
        "breakfast": {"start": "08:00", "end": "10:00", "capacity_orders_per_15m": 24},
        "lunch": {"start": "12:00", "end": "14:30", "capacity_orders_per_15m": 30},
        "dinner": {"start": "19:00", "end": "21:30", "capacity_orders_per_15m": 24}
    }
}
(RAW / "shift_config.json").write_text(json.dumps(shift_config, indent=2))

# Third source: the staff rostering system's database (SQLite stands in for its SQL server).
# Uses its own RNG so the order random stream is unchanged. A short-staffed shift slows preparation below.
roster_rng = random.Random(20252)
roster_path = RAW / "staff_roster.db"
if roster_path.exists(): roster_path.unlink()
db = sqlite3.connect(roster_path)
db.execute("""CREATE TABLE staff_roster(roster_date TEXT NOT NULL, shift TEXT NOT NULL,
              staff_scheduled INTEGER NOT NULL, staff_present INTEGER NOT NULL, PRIMARY KEY(roster_date, shift))""")
scheduled = {"breakfast": 4, "lunch": 5, "dinner": 4}
roster_rows = []
for day in range(7):
    d = (datetime(2026, 9, 7) + timedelta(days=day)).date().isoformat()
    for shift, n in scheduled.items():
        absent = roster_rng.choices([0, 1], [0.75, 0.25])[0]  # roughly one shift in four runs a person short
        roster_rows.append((d, shift, n, n - absent))
short_staffed = {(d, sh): present < n for d, sh, n, present in roster_rows}
db.executemany("INSERT INTO staff_roster VALUES(?,?,?,?)", roster_rows)
db.commit(); db.close()

fields = ["order_id", "order_ts", "accepted_ts", "ready_ts", "handover_ts", "shift", "channel", "item_count", "cancelled", "issue_code"]
base = datetime(2026, 9, 7)
rows = []
order_no = 1
for day in range(7):
    for shift, start_hour, count in [("breakfast", 8, 85), ("lunch", 12, 125), ("dinner", 19, 115)]:
        for _ in range(count):
            minute = random.randrange(0, 120 if shift == "breakfast" else 150)
            order_ts = base + timedelta(days=day, hours=start_hour, minutes=minute, seconds=random.randrange(60))
            peak = minute in range(30, 91)
            accepted_delay = max(0.2, random.gauss(1.3 if peak else 0.8, 0.4))
            prep = max(2.0, random.gauss(10.0 if peak else 7.0, 2.7))
            handover = max(0.3, random.gauss(2.2 if peak else 1.2, 0.8))
            cancelled = random.random() < (0.045 if peak else 0.02)
            issue = random.choices(["", "stockout", "payment", "kitchen_rework"], [0.92, 0.025, 0.025, 0.03])[0]
            if issue == "kitchen_rework": prep += random.uniform(5, 10)
            d_key = (base + timedelta(days=day)).date().isoformat()
            if short_staffed[(d_key, shift)]: prep *= 1.15  # one person short -> ~15% slower prep (synthetic assumption)
            accepted_ts = order_ts + timedelta(minutes=accepted_delay)
            ready_ts = accepted_ts + timedelta(minutes=prep)
            handover_ts = "" if cancelled else (ready_ts + timedelta(minutes=handover)).isoformat()
            rows.append({
                "order_id": f"ORD-{order_no:05d}", "order_ts": order_ts.isoformat(),
                "accepted_ts": accepted_ts.isoformat(), "ready_ts": ready_ts.isoformat(),
                "handover_ts": handover_ts, "shift": shift,
                "channel": random.choice(["counter", "app"]), "item_count": random.randint(1, 4),
                "cancelled": str(cancelled).lower(), "issue_code": issue
            })
            order_no += 1

# Planted defects (appended after the random loop, so the clean orders and KPI are unchanged).
# Each one exercises a specific validation rule so the quarantine evidence is visible.
good = rows[1]
rows += [
    dict(rows[0]),                                                                # exact duplicate export row
    {**good, "order_id": "BAD-MISSING", "order_ts": "", "accepted_ts": "", "ready_ts": "", "handover_ts": ""},
    {**good, "order_id": "BAD-NO-HANDOVER", "handover_ts": ""},                   # completed but never handed over
    {**good, "order_id": "BAD-SEQUENCE", "ready_ts": good["order_ts"]},           # ready before accepted
    {**good, "order_id": "BAD-SHIFT", "shift": "brunch"},                         # not in shift config
    {**good, "order_id": "BAD-SHIFT-WINDOW", "shift": "dinner"},                  # 09:40 order labelled dinner
    {**good, "order_id": "BAD-FLAG", "cancelled": "yes"},                         # non-boolean flag
    {**good, "order_id": "BAD-TIMESTAMP", "accepted_ts": "07/09/2026 09:41"},     # unparseable timestamp
]

orders_path = RAW / "orders.csv"
with orders_path.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader(); writer.writerows(rows)

# Source-side control totals: what each "exporting system" says it sent.
manifest = {
    "orders_csv": {"row_count": len(rows), "sha256": hashlib.sha256(orders_path.read_bytes()).hexdigest(),
                   "period_start": "2026-09-07", "period_end": "2026-09-13"},
    "staff_roster_db": {"table": "staff_roster", "row_count": len(roster_rows),
                        "period_start": "2026-09-07", "period_end": "2026-09-13"},
}
(RAW / "manifest.json").write_text(json.dumps(manifest, indent=2))
print(f"Generated {len(rows)} synthetic order rows and {len(roster_rows)} roster rows in {RAW}")

