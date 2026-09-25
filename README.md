# Cafeteria wait-time: a dependable operational metric pipeline

Submitted by: Shreyansh Arora, 24bcs10252, shreyansh.24bcs10252@sst.scaler.com

An FDE Data Foundations project (Track C). It takes fragmented cafeteria order events, shift configuration and a staff roster database and turns them into a validated, repeatable service-time KPI. All data is synthetic and fixed-seed (seed `10252`), so the whole path is reproducible without exposing student data.

## Problem

Students order at the counter or in an app. Staff accept the order, the kitchen prepares it, and the handover desk calls the student. Today, delays are noticed only when a student complains. There is no dependable view of whether orders meet the service target, or at which stage delay enters the workflow.

## Users, stakeholders and the decision supported

| Stakeholder | Need |
|---|---|
| Cafeteria manager (decision owner) | Which shift and which stage need intervention this week |
| Kitchen and handover staff | Which stage is causing delay |
| Students | Predictable pickup times |

**Decision supported:** a weekly staffing/process decision - which shift and which workflow stage should the manager fix first to raise on-time handover?

## Project KPI and results

**Project KPI:** percentage of completed (non-cancelled) orders handed over within 12 minutes of being placed.

| # | Metric | Result on included data |
|---|---|---|
| 1 | Completed orders within 12 min (KPI) | **57.55%** |
| 2 | Average total wait, split into accept / prepare / handover | 11.44 min (1.03 / 8.74 / 1.66) |
| 3 | Cancellation rate | 3.08% |
| 4 | Issue rate, and within-target rate with vs without an issue | 8.66% (44.44% vs 58.78%) |
| 5 | Within-target rate by shift | Breakfast 52.24%, Dinner 56.63%, Lunch 62.03% |
|   | Within-target rate, short-staffed vs fully staffed shifts (from the roster) | 50.89% vs 59.54% |

Preparation is about three-quarters of the wait. Shifts that ran a person short (5 of 21, per the roster) hit the target about 9 points less often, and the gap holds within every shift (3-13 points), so covering absences is the first lever to test. Breakfast is the weakest shift. Full table, shift detail and Known / Unknown / Assumption / Limitation: [`evidence_table.md`](evidence_table.md).

## Sources

| Source | Retrieval mode | Grain / key | Owner | Completeness evidence |
|---|---|---|---|---|
| `data/raw/orders.csv` | CSV file export | one row per order / `order_id` | Operations: order events and milestones | Row count and SHA-256 checked against `manifest.json`; run stops on mismatch |
| `data/raw/shift_config.json` | JSON configuration | one object per shift / `shift` | Cafeteria manager: target, capacity, shift hours | Required keys checked; run stops if missing |
| `data/raw/staff_roster.db` (table `staff_roster`) | SQL query (read-only SQLite connection, parameterised by the manifest period) | one row per day and shift / (`roster_date`, `shift`) | Rostering system: staff scheduled and present | Row count vs manifest; every day has every shift; 0 < present <= scheduled; run stops on any failure |
| `data/raw/manifest.json` | JSON control totals | one entry per export | Exporting system | Expected row count, checksum and period |

The pipeline uses three retrieval types: a CSV file export, a JSON configuration and a SQL database query. SQLite stands in for the rostering system's SQL server, so the same query would run against a real database with only the connection changed. Business question -> information -> source mapping: [`source_map.md`](source_map.md). Entities, events and the data model diagram: [`workflow_model.md`](workflow_model.md).

## Setup and run

Requires Python 3.9+ (standard library only).

```bash
python3 generate_data.py          # writes data/raw/ (orders.csv, shift_config.json, staff_roster.db, manifest.json)
python3 pipeline.py               # ingest -> validate -> model -> metrics
python3 -m unittest discover -v   # 17 tests
```

Outputs in `data/processed/`: `summary.json` (metrics), `order_metrics.csv` (clean orders with stage durations), `workflow.db` (SQLite: `orders`, `shifts`, `shift_staffing`), `quality_report.json` (every rejected row with its line number and reason). Logs go to `logs/pipeline.log`.

## How the pipeline stays dependable

- **Ingest:** checks the CSV header once before reading any row, then checks row count and SHA-256 against `manifest.json`. Queries the roster database read-only for the manifest's period and checks its row count and coverage. Raw files are never modified.
- **Validate:** row-level rules quarantine bad rows instead of fixing them: missing or unparseable timestamps, out-of-order milestones, a completed order with no handover, a cancelled order with a handover, an unknown shift, an order time outside its shift window, an order whose day/shift has no roster entry, invalid `cancelled` / `channel` / `issue_code` values, a bad `item_count`. Exact duplicates keep one copy; conflicting duplicates reject every copy.
- **Model and metrics:** the within-target flag compares the raw wait time, and rounds only for display, so an order at 12.004 minutes is not counted as on time.
- **Failure handling:** source- or schema-level problems stop the run with exit code 1 and a logged reason. Outputs are written to temporary files and swapped in only after a full successful run, so a failed run leaves the previous outputs untouched.
- **Reruns:** the same inputs give the same metrics and rejected rows; only the run ID and timestamp change.

## Judgement call

A completed order with no handover time is quarantined, not imputed. Filling it in would silently change the KPI's denominator, and the manager needs to see that the data has a gap.

## Limitations

- All data is synthetic. Shift differences come from the generator's parameters, so they show that the pipeline works, not a real-world finding.
- The 12-minute target needs confirmation from the cafeteria manager. The ~15% slower preparation on short-staffed shifts is a generator assumption, so the staffing gap shows the pipeline can detect such an effect, not that it exists.
- Queue length, menu complexity and mid-shift staffing changes are not in the sources.
- Next step: point the pipeline at real exports and the real roster database, and add freshness alerts.

## Repository contents

`problem_brief.md` · `source_map.md` · `workflow_model.md` · `evidence_table.md` · `demo_script.md` · `generate_data.py` · `pipeline.py` · `notebook.ipynb` · `tests/`
