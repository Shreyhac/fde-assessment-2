"""Cafeteria wait-time pipeline: ingest (CSV + JSON + SQL) -> validate -> transform/model -> metric output.


Usage: python3 pipeline.py [raw_dir] [out_dir]   (defaults: data/raw, data/processed)
Exit code 0 = success, 1 = run stopped (previous outputs are left untouched).
"""
import csv, hashlib, json, logging, os, sqlite3, sys, tempfile, uuid
from datetime import datetime, time
from pathlib import Path

ROOT = Path(__file__).parent
REQUIRED = ["order_id", "order_ts", "accepted_ts", "ready_ts", "handover_ts",
            "shift", "channel", "item_count", "cancelled", "issue_code"]
ALLOWED = {"channel": {"counter", "app"}, "cancelled": {"true", "false"},
           "issue_code": {"", "stockout", "payment", "kitchen_rework"}}
log = logging.getLogger("pipeline")


class PipelineError(Exception):
    """Source- or schema-level failure: stop the run, write nothing."""


def minutes(a, b):
    return (b - a).total_seconds() / 60


# ---------- ingest ----------
ROSTER_SQL = """SELECT roster_date, shift, staff_scheduled, staff_present
                FROM staff_roster WHERE roster_date BETWEEN ? AND ? ORDER BY roster_date, shift"""


def load_roster(raw, manifest, config):
    """Retrieve staffing from the rostering system's SQL database for the order export's period."""
    spec = manifest["staff_roster_db"]
    try:
        conn = sqlite3.connect(f"file:{raw / 'staff_roster.db'}?mode=ro", uri=True)
        try:
            result = conn.execute(ROSTER_SQL, (spec["period_start"], spec["period_end"])).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        raise PipelineError(f"staff_roster.db query failed: {e}")
    if len(result) != spec["row_count"]:
        raise PipelineError(f"roster rows {len(result)} != manifest {spec['row_count']} (incomplete roster?)")
    roster, seen = {}, set()
    for d, shift, sched, present in result:
        if shift not in config["shifts"]:
            raise PipelineError(f"roster has unknown shift {shift!r} on {d}")
        if not (isinstance(sched, int) and isinstance(present, int) and 0 < present <= sched):
            raise PipelineError(f"roster values invalid for {d} {shift}: scheduled={sched} present={present}")
        roster[(d, shift)] = {"staff_scheduled": sched, "staff_present": present}
        seen.add(d)
    # Every day in the period must have every shift, or staffing metrics would silently drop orders.
    missing = [(d, s) for d in sorted(seen) for s in config["shifts"] if (d, s) not in roster]
    if missing:
        raise PipelineError(f"roster missing day/shift slots: {missing}")
    return roster


def load_sources(raw):
    for name in ("orders.csv", "shift_config.json", "manifest.json", "staff_roster.db"):
        if not (raw / name).exists():
            raise PipelineError(f"missing source file: {raw / name}")
    config = json.loads((raw / "shift_config.json").read_text())
    if "service_target_minutes" not in config or not config.get("shifts"):
        raise PipelineError("shift_config.json lacks service_target_minutes or shifts")
    full_manifest = json.loads((raw / "manifest.json").read_text())
    if "staff_roster_db" not in full_manifest:
        raise PipelineError("manifest.json lacks staff_roster_db control totals")
    manifest = full_manifest["orders_csv"]
    data = (raw / "orders.csv").read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    reader = csv.DictReader(data.decode("utf-8").splitlines())
    missing = [c for c in REQUIRED if c not in (reader.fieldnames or [])]
    if missing:
        raise PipelineError(f"orders.csv header missing columns: {missing}")
    rows = list(reader)
    # Completeness: compare what we received against the exporter's control totals.
    if len(rows) != manifest["row_count"]:
        raise PipelineError(f"row count {len(rows)} != manifest {manifest['row_count']} (incomplete export?)")
    if sha != manifest["sha256"]:
        raise PipelineError("orders.csv checksum does not match manifest (file altered or truncated)")
    roster = load_roster(raw, full_manifest, config)
    log.info("Retrieved %d CSV rows (sha256 %s..., matches manifest), JSON config with %d shifts, "
             "and %d roster rows via SQL", len(rows), sha[:12], len(config["shifts"]), len(roster))
    return rows, config, roster, {"orders_csv_sha256": sha, "manifest_row_count": manifest["row_count"],
                                  "roster_rows": len(roster)}


# ---------- validate ----------
def in_window(ts, spec):
    return time.fromisoformat(spec["start"]) <= ts.time() < time.fromisoformat(spec["end"])


def validate_row(r, config, roster=None):
    """Return (clean_record, None) or (None, [error codes]). Never imputes values."""
    errors = []
    if not r.get("order_id"): errors.append("missing_order_id")
    if not all(r.get(k) for k in ("order_ts", "accepted_ts", "ready_ts")): errors.append("missing_core_timestamp")
    for col, allowed in ALLOWED.items():
        if (r.get(col) or "").strip().lower() not in allowed: errors.append(f"invalid_{col}")
    if r.get("shift") not in config["shifts"]: errors.append("invalid_shift")
    if not (r.get("item_count") or "").isdigit() or int(r["item_count"]) < 1: errors.append("invalid_item_count")
    if errors: return None, errors

    cancelled = r["cancelled"].strip().lower() == "true"
    try:
        ts = {k: datetime.fromisoformat(r[k]) for k in ("order_ts", "accepted_ts", "ready_ts")}
        handover = datetime.fromisoformat(r["handover_ts"]) if r["handover_ts"] else None
    except ValueError:
        return None, ["invalid_timestamp"]
    if not cancelled and handover is None: errors.append("completed_without_handover")
    if cancelled and handover is not None: errors.append("cancelled_with_handover")
    if not (ts["order_ts"] <= ts["accepted_ts"] <= ts["ready_ts"]) or (handover and ts["ready_ts"] > handover):
        errors.append("timestamp_sequence")
    if not in_window(ts["order_ts"], config["shifts"][r["shift"]]): errors.append("shift_window_mismatch")
    if roster is not None and (ts["order_ts"].date().isoformat(), r["shift"]) not in roster:
        errors.append("no_roster_for_shift")
    if errors: return None, errors

    target = config["service_target_minutes"]
    wait = minutes(ts["order_ts"], handover) if handover else None
    return {
        "order_id": r["order_id"], "order_date": ts["order_ts"].date().isoformat(), "shift": r["shift"],
        "channel": r["channel"].strip().lower(), "item_count": int(r["item_count"]), "cancelled": int(cancelled),
        "issue_code": r["issue_code"].strip().lower(),
        "accept_min": round(minutes(ts["order_ts"], ts["accepted_ts"]), 2),
        "prep_min": round(minutes(ts["accepted_ts"], ts["ready_ts"]), 2),
        "handover_min": round(minutes(ts["ready_ts"], handover), 2) if handover else None,
        "wait_minutes": round(wait, 2) if wait is not None else None,
        "within_target": int(wait <= target) if wait is not None else None,  # compare unrounded
    }, None


def validate(rows, config, roster=None):
    counts = {}
    for r in rows: counts[r.get("order_id")] = counts.get(r.get("order_id"), 0) + 1
    clean, rejected, kept_dupes = [], [], set()
    for line, r in enumerate(rows, start=2):  # line 1 is the header
        oid = r.get("order_id")
        if oid and counts[oid] > 1:
            copies = [x for x in rows if x.get("order_id") == oid]
            if any(c != copies[0] for c in copies):  # conflicting versions: no safe canonical record
                rejected.append({"line": line, "order_id": oid, "errors": ["conflicting_duplicate_order_id"], "raw": r}); continue
            if oid in kept_dupes:  # exact re-export: keep first copy, record the drop
                rejected.append({"line": line, "order_id": oid, "errors": ["exact_duplicate_dropped"], "raw": r}); continue
            kept_dupes.add(oid)
        rec, errors = validate_row(r, config, roster)
        if errors: rejected.append({"line": line, "order_id": oid, "errors": errors, "raw": r})
        else: clean.append(rec)
    if not clean:
        raise PipelineError("no rows passed validation")
    return clean, rejected


# ---------- model + metrics ----------
def build_model(db_path, clean, config, roster):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE shifts(shift TEXT PRIMARY KEY, start_time TEXT, end_time TEXT,
                            capacity_orders_per_15m INTEGER, shift_hours REAL);
        CREATE TABLE shift_staffing(roster_date TEXT, shift TEXT REFERENCES shifts(shift),
                            staff_scheduled INTEGER, staff_present INTEGER, PRIMARY KEY(roster_date, shift));
        CREATE TABLE orders(order_id TEXT PRIMARY KEY, order_date TEXT, shift TEXT REFERENCES shifts(shift),
                            channel TEXT, item_count INTEGER, cancelled INTEGER, issue_code TEXT,
                            accept_min REAL, prep_min REAL, handover_min REAL, wait_minutes REAL, within_target INTEGER);""")
    for name, s in config["shifts"].items():
        hours = minutes(datetime.combine(datetime.min, time.fromisoformat(s["start"])),
                        datetime.combine(datetime.min, time.fromisoformat(s["end"]))) / 60
        conn.execute("INSERT INTO shifts VALUES(?,?,?,?,?)", (name, s["start"], s["end"], s["capacity_orders_per_15m"], hours))
    conn.executemany("INSERT INTO shift_staffing VALUES(?,?,?,?)",
                     [(d, sh, v["staff_scheduled"], v["staff_present"]) for (d, sh), v in roster.items()])
    cols = list(clean[0].keys())
    conn.executemany(f"INSERT INTO orders({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                     [tuple(r[c] for c in cols) for r in clean])
    conn.commit()
    return conn


def compute_metrics(conn, target):
    q = lambda sql: conn.execute(sql).fetchone()
    completed, within = q("SELECT COUNT(*), SUM(within_target) FROM orders WHERE cancelled=0")
    avg = q("SELECT ROUND(AVG(wait_minutes),2), ROUND(AVG(accept_min),2), ROUND(AVG(prep_min),2), ROUND(AVG(handover_min),2) FROM orders WHERE cancelled=0")
    issue_split = {("with_issue" if k else "no_issue"): v for k, v in conn.execute(
        "SELECT issue_code<>'', ROUND(100.0*AVG(within_target),2) FROM orders WHERE cancelled=0 GROUP BY 1")}
    # Staff-hours come from the roster (actual people present per day and shift), not a fixed config value.
    shift_metrics = [dict(zip(["shift", "completed_orders", "within_target_pct", "avg_wait_minutes", "avg_accept_min",
                               "avg_prep_min", "avg_handover_min", "avg_staff_present", "orders_per_staff_hour"], row))
                     for row in conn.execute("""
        WITH staff AS (SELECT st.shift, AVG(st.staff_present) AS avg_present,
                              SUM(st.staff_present * s.shift_hours) AS staff_hours
                       FROM shift_staffing st JOIN shifts s ON s.shift = st.shift GROUP BY st.shift)
        SELECT o.shift, SUM(o.cancelled=0), ROUND(100.0*AVG(o.within_target),2), ROUND(AVG(o.wait_minutes),2),
               ROUND(AVG(o.accept_min),2), ROUND(AVG(o.prep_min),2), ROUND(AVG(o.handover_min),2),
               ROUND(staff.avg_present,2), ROUND(1.0*COUNT(*)/staff.staff_hours,2)
        FROM orders o JOIN staff ON staff.shift = o.shift GROUP BY o.shift ORDER BY 3""")]
    staffing_split = {("short_staffed" if short else "fully_staffed"): {"shift_days": sd, "completed_orders": n, "within_target_pct": pct}
                      for short, sd, n, pct in conn.execute("""
        SELECT st.staff_present < st.staff_scheduled, COUNT(DISTINCT st.roster_date||st.shift),
               SUM(o.cancelled=0), ROUND(100.0*AVG(o.within_target),2)
        FROM orders o JOIN shift_staffing st ON st.roster_date = o.order_date AND st.shift = o.shift
        GROUP BY 1""")}
    # Same comparison inside each shift, so shift mix cannot explain the gap.
    staffing_by_shift = {}
    for shift, short, n, pct in conn.execute("""
        SELECT o.shift, st.staff_present < st.staff_scheduled, SUM(o.cancelled=0), ROUND(100.0*AVG(o.within_target),2)
        FROM orders o JOIN shift_staffing st ON st.roster_date = o.order_date AND st.shift = o.shift
        GROUP BY 1, 2 ORDER BY 1, 2"""):
        staffing_by_shift.setdefault(shift, {})["short_staffed" if short else "fully_staffed"] = {
            "completed_orders": n, "within_target_pct": pct}
    return {
        "project_kpi": f"percent of completed orders handed over within {target} minutes",
        "project_kpi_value_pct": round(100 * within / completed, 2),
        "completed_orders": completed,
        "average_wait_minutes": avg[0],
        "average_stage_minutes": {"accept": avg[1], "prepare": avg[2], "handover": avg[3]},
        "cancellation_rate_pct": q("SELECT ROUND(100.0*AVG(cancelled),2) FROM orders")[0],
        "issue_rate_pct": q("SELECT ROUND(100.0*AVG(issue_code<>''),2) FROM orders")[0],
        "within_target_pct_by_issue": issue_split,
        "shift_metrics": shift_metrics,
        "within_target_by_staffing": staffing_split,
        "within_target_by_staffing_and_shift": staffing_by_shift,
    }


# ---------- output ----------
def atomic_write(path, text):
    tmp = path.with_suffix(path.suffix + ".tmp"); tmp.write_text(text); os.replace(tmp, path)


def run(raw=ROOT / "data" / "raw", out=ROOT / "data" / "processed"):
    run_id = uuid.uuid4().hex[:8]
    log.info("run %s started", run_id)
    rows, config, roster, lineage = load_sources(raw)
    clean, rejected = validate(rows, config, roster)
    reasons = {}
    for rej in rejected:
        for e in rej["errors"]: reasons[e] = reasons.get(e, 0) + 1
    log.info("Accepted %d rows; rejected %d; reasons %s", len(clean), len(rejected), reasons)

    out.mkdir(parents=True, exist_ok=True)
    fd, tmp_db = tempfile.mkstemp(suffix=".db", dir=out); os.close(fd); os.remove(tmp_db)
    conn = build_model(tmp_db, clean, config, roster)
    try:
        summary = compute_metrics(conn, config["service_target_minutes"])
    finally:
        conn.close()
    meta = {"run_id": run_id, "input_rows": len(rows), **lineage}
    summary["run"] = meta
    quality = {**meta, "accepted_rows": len(clean), "rejected_rows": len(rejected), "rejections_by_reason": reasons,
               "rejections": rejected,
               "checks": ["required columns present (run stops)", "row count and sha256 match manifest (run stops)",
                          "order_id present and unique", "core timestamps present and parseable",
                          "completed orders have handover; cancelled orders do not", "chronological milestones",
                          "shift exists in config", "order time falls inside shift window",
                          "channel / cancelled / issue_code in allowed values", "item_count positive integer",
                          "roster retrieved by SQL for the manifest period; row count matches manifest (run stops)",
                          "roster covers every day/shift; 0 < staff_present <= staff_scheduled (run stops)",
                          "every order's day/shift has a roster slot"]}

    # Everything computed successfully -> publish all outputs together.
    os.replace(tmp_db, out / "workflow.db")
    tmp_csv = out / "order_metrics.csv.tmp"
    with tmp_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=clean[0].keys()); w.writeheader(); w.writerows(clean)
    os.replace(tmp_csv, out / "order_metrics.csv")
    atomic_write(out / "quality_report.json", json.dumps(quality, indent=2))
    atomic_write(out / "summary.json", json.dumps(summary, indent=2))
    log.info("run %s finished; KPI %.2f%%", run_id, summary["project_kpi_value_pct"])
    return summary, quality


def main(argv):
    log_path = Path(os.environ.get("PIPELINE_LOG", ROOT / "logs" / "pipeline.log"))  # tests redirect this
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True,
                        handlers=[logging.FileHandler(log_path), logging.StreamHandler()])
    args = [Path(a) for a in argv[1:3]]
    try:
        summary, quality = run(*args)
    except PipelineError as e:
        log.error("RUN STOPPED: %s (previous outputs left untouched)", e); return 1
    except Exception:
        log.exception("RUN FAILED with unexpected error (previous outputs left untouched)"); return 1
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

