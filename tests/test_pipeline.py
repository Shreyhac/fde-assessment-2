import csv, json, os, shutil, sqlite3, subprocess, sys, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pipeline  # noqa: E402

CONFIG = {"service_target_minutes": 12,
          "shifts": {"lunch": {"start": "12:00", "end": "14:30", "capacity_orders_per_15m": 30}}}
GOOD = {"order_id": "T1", "order_ts": "2026-09-07T12:00:00", "accepted_ts": "2026-09-07T12:01:00",
        "ready_ts": "2026-09-07T12:09:00", "handover_ts": "2026-09-07T12:11:00", "shift": "lunch",
        "channel": "app", "item_count": "2", "cancelled": "false", "issue_code": ""}


class ValidationRules(unittest.TestCase):
    def check(self, expected_error, **changes):
        rec, errors = pipeline.validate_row({**GOOD, **changes}, CONFIG)
        self.assertIsNone(rec); self.assertIn(expected_error, errors)


    def test_good_row_accepted(self):
        rec, errors = pipeline.validate_row(GOOD, CONFIG)
        self.assertIsNone(errors); self.assertEqual(rec["wait_minutes"], 11.0); self.assertEqual(rec["within_target"], 1)

    def test_completed_order_needs_handover(self):  self.check("completed_without_handover", handover_ts="")
    def test_cancelled_order_has_no_handover(self): self.check("cancelled_with_handover", cancelled="true")
    def test_chronology(self):                      self.check("timestamp_sequence", ready_ts="2026-09-07T12:00:30")
    def test_unparseable_timestamp(self):           self.check("invalid_timestamp", accepted_ts="07/09/2026 12:01")
    def test_unknown_shift(self):                   self.check("invalid_shift", shift="brunch")
    def test_shift_window(self):                    self.check("shift_window_mismatch", order_ts="2026-09-07T09:00:00",
                                                               accepted_ts="2026-09-07T09:01:00")
    def test_bad_cancelled_flag(self):              self.check("invalid_cancelled", cancelled="yes")
    def test_bad_item_count(self):                  self.check("invalid_item_count", item_count="two")

    def test_target_boundary_not_rounded(self):
        rec, _ = pipeline.validate_row({**GOOD, "handover_ts": "2026-09-07T12:12:00.200000"}, CONFIG)  # 12.003 min
        self.assertEqual(rec["within_target"], 0)

    def test_conflicting_duplicates_rejected_exact_duplicates_deduped(self):
        clean, rejected = pipeline.validate([GOOD, dict(GOOD), {**GOOD, "order_id": "T2"}, {**GOOD, "order_id": "T2", "item_count": "3"}], CONFIG)
        self.assertEqual([r["order_id"] for r in clean], ["T1"])
        self.assertEqual(sorted(e for r in rejected for e in r["errors"]),
                         ["conflicting_duplicate_order_id"] * 2 + ["exact_duplicate_dropped"])


class RosterRules(unittest.TestCase):
    def test_order_without_roster_slot_is_quarantined(self):
        roster = {("2026-09-08", "lunch"): {"staff_scheduled": 5, "staff_present": 5}}
        rec, errors = pipeline.validate_row(GOOD, CONFIG, roster)  # GOOD is on 2026-09-07
        self.assertIsNone(rec); self.assertIn("no_roster_for_shift", errors)

    def test_order_with_roster_slot_passes(self):
        roster = {("2026-09-07", "lunch"): {"staff_scheduled": 5, "staff_present": 4}}
        rec, errors = pipeline.validate_row(GOOD, CONFIG, roster)
        self.assertIsNone(errors)


class EndToEnd(unittest.TestCase):
    """Runs against a temp copy so the repo's raw inputs and outputs are never touched."""
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp()); self.raw, self.out = self.tmp / "raw", self.tmp / "out"
        subprocess.run([sys.executable, str(ROOT / "generate_data.py"), str(self.raw)], check=True, capture_output=True)

    def tearDown(self): shutil.rmtree(self.tmp)

    def run_pipeline(self):
        return subprocess.run([sys.executable, str(ROOT / "pipeline.py"), str(self.raw), str(self.out)], capture_output=True, text=True,
                              env={**os.environ, "PIPELINE_LOG": str(self.tmp / "test.log")})  # keep the repo log clean

    def test_expected_output_and_quarantine(self):
        self.assertEqual(self.run_pipeline().returncode, 0)
        summary = json.loads((self.out / "summary.json").read_text())
        quality = json.loads((self.out / "quality_report.json").read_text())
        self.assertEqual((quality["accepted_rows"], quality["rejected_rows"]), (2275, 8))
        self.assertEqual(summary["project_kpi_value_pct"], 57.55)
        self.assertEqual(quality["input_rows"], quality["accepted_rows"] + quality["rejected_rows"])
        self.assertEqual(quality["roster_rows"], 21)  # 7 days x 3 shifts retrieved via SQL
        split = summary["within_target_by_staffing"]
        self.assertEqual(split["short_staffed"]["shift_days"] + split["fully_staffed"]["shift_days"], 21)

    def roster_sql(self, sql):
        db = sqlite3.connect(self.raw / "staff_roster.db"); db.execute(sql); db.commit(); db.close()

    def test_roster_failures_stop_run(self):
        self.run_pipeline(); before = (self.out / "summary.json").read_text()
        # 1) a missing day/shift slot in the SQL source
        self.roster_sql("DELETE FROM staff_roster WHERE roster_date='2026-09-09' AND shift='dinner'")
        r = self.run_pipeline(); self.assertEqual(r.returncode, 1); self.assertIn("roster rows 20", r.stderr)
        # 2) impossible staffing value (more present than scheduled)
        self.roster_sql("INSERT INTO staff_roster VALUES('2026-09-09','dinner',4,9)")
        r = self.run_pipeline(); self.assertEqual(r.returncode, 1); self.assertIn("roster values invalid", r.stderr)
        # 3) table missing entirely
        self.roster_sql("DROP TABLE staff_roster")
        r = self.run_pipeline(); self.assertEqual(r.returncode, 1); self.assertIn("query failed", r.stderr)
        self.assertEqual((self.out / "summary.json").read_text(), before)

    def test_rerun_is_idempotent(self):
        self.run_pipeline(); first = json.loads((self.out / "summary.json").read_text())
        self.run_pipeline(); second = json.loads((self.out / "summary.json").read_text())
        first.pop("run"); second.pop("run")
        self.assertEqual(first, second)

    def test_failures_stop_run_and_keep_previous_outputs(self):
        self.run_pipeline(); before = (self.out / "summary.json").read_text()
        orders = self.raw / "orders.csv"
        # 1) truncated export -> manifest mismatch
        orders.write_text("\n".join(orders.read_text().splitlines()[:-5]) + "\n")
        r = self.run_pipeline(); self.assertEqual(r.returncode, 1); self.assertIn("manifest", r.stderr)
        # 2) missing file
        orders.unlink()
        r = self.run_pipeline(); self.assertEqual(r.returncode, 1); self.assertIn("missing source file", r.stderr)
        self.assertEqual((self.out / "summary.json").read_text(), before)


if __name__ == "__main__":
    unittest.main()

