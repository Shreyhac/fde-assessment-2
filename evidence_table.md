# Final evidence table

Produced by `python3 pipeline.py` from the fixed-seed synthetic inputs: 2,283 input rows, 2,275 accepted, 8 quarantined. Source: `data/processed/summary.json`.

| # | Metric | Result | Decision relevance |
|---|---|---|---|
| 1 | Completed orders handed over within 12 min (project KPI) | **57.55%** (1,269 of 2,205) | Current service-target reliability |
| 2 | Average total wait (completed orders) | 11.44 min | Overall delay size |
|   | accept / prepare / handover | 1.03 / 8.74 / 1.66 min | Preparation is ~76% of the wait: the stage to target first |
| 3 | Cancellation rate | 3.08% | Failed outcomes |
| 4 | Issue rate | 8.66% | Exception workload |
|   | within target: no issue vs with issue | 58.78% vs 44.44% | Exceptions are linked to worse service |
| 5 | Within-target rate by shift | Breakfast 52.24% / Dinner 56.63% / Lunch 62.03% | Where to intervene first |
|   | within target: short-staffed (5 shift-days, 507 orders) vs fully staffed (16 shift-days, 1,698 orders) | 50.89% vs 59.54% | Covering absences is the first lever to test |

## Shift detail (joined to shift configuration and the SQL roster)

| Shift | Completed | Within target | Avg wait | Accept | Prepare | Handover | Avg staff present | Orders per staff-hour |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Breakfast | 580 | 52.24% | 11.92 | 1.08 | 9.18 | 1.72 | 3.71 | 11.44 |
| Dinner | 777 | 56.63% | 11.43 | 1.03 | 8.78 | 1.65 | 3.71 | 12.38 |
| Lunch | 848 | 62.03% | 11.12 | 1.01 | 8.47 | 1.63 | 4.86 | 10.29 |

## Staffing gap within each shift (from `within_target_by_staffing_and_shift`)

| Shift | Fully staffed: orders / within target | Short-staffed: orders / within target | Gap |
|---|---:|---:|---:|
| Breakfast | 416 / 54.81% | 164 / 45.73% | 9.1 pts |
| Dinner | 553 / 57.50% | 224 / 54.46% | 3.0 pts |
| Lunch | 729 / 63.79% | 119 / 51.26% | 12.5 pts |

The gap appears within every shift (3-13 points), so it is not only shift mix. It rests on just 5 short-staffed shift-days, so treat it as a signal to test, not a finding.

## Known / Unknown / Assumption / Limitation

- **Known:** in this run, preparation is the largest stage of the wait in every shift, and orders with an issue code hit the target less often (44.44% vs 58.78%). The short-staffed gap (3-13 points within each shift) is a pattern the pipeline detects, not an established fact.
- **Unknown:** queue length, menu complexity and mid-shift staffing changes are not in the sources.
- **Assumption:** the 12-minute target still needs confirmation from the cafeteria manager. The generator slows preparation ~15% on short-staffed shifts.
- **Limitation:** all data is synthetic (seed 10252). Shift and staffing differences come from the generator's parameters, so they show that the pipeline works, not a real finding. Breakfast's short 2-hour shift holds proportionally more peak-window orders.
