# Problem brief: dependable cafeteria wait-time visibility

## Stakeholders and current workflow
Students place orders at the counter or in an app. Cafeteria staff accept each order, the kitchen prepares it, and a handover desk calls the student. Students want predictable pickup times; the cafeteria manager owns staffing, throughput and service quality; kitchen and handover staff need a clear queue. Today, a delay may be noticed only after a student complains. Assumptions: each workflow milestone can be timestamped and a 12-minute order-to-handover target is acceptable. Unknowns to validate with stakeholders: the real target, whether app and counter queues differ, staffing breaks, and which issue codes are consistently recorded.

## Problem statement and workflow gap
There is no dependable daily view showing where delay enters the order workflow or whether orders meet the service target. The workflow is order placed -> accepted -> prepared -> ready -> handed over. Delay can enter at acceptance, preparation, rework/stockout, or handover. This project creates a repeatable pipeline from raw events to service metrics using reproducible synthetic data.

## KPI, scope and success
**Project KPI:** percentage of completed orders handed over within 12 minutes. Supporting metrics: average total wait split into accept / prepare / handover stages, cancellation rate, issue rate, within-target rate by shift, and within-target rate on short-staffed vs fully staffed shifts (from the roster). MVP scope covers one cafeteria, one week, breakfast/lunch/dinner, app/counter orders and order-level milestones. Out of scope: demand forecasting, menus, payments, student identity, and production deployment. Success means the pipeline runs from raw sources without manual edits, rejects unsafe rows, logs its run and produces actionable shift metrics.

## Data and source-of-truth choices
The order-event CSV is the source of truth for operational events. A JSON configuration is the source of truth for shift windows, capacity and the 12-minute target. A roster database (queried with SQL) is the source of truth for staff scheduled and present per day and shift. All three are synthetically generated with a fixed seed and documented logic. Key fields are order ID, milestone timestamps, shift, channel, item count, cancellation and issue code. Missing real-world fields include mid-shift staffing changes, menu complexity and queue length.

## Retrieval, cleaning and model
The pipeline retrieves three source types: CSV events, JSON configuration and a SQL roster table; preserves the raw sources; validates required fields, unique IDs, timestamp parsing, valid shifts and timestamp order, and writes rejected rows to a quality report. Safe transforms derive total wait and target compliance; no timestamp is imputed. The model contains order entities; placed, accepted, ready and handover events; staffing/issue interventions; completion/cancellation outcomes; and shift relationships. Clean order data is loaded into SQLite, where joins/aggregations can produce the metrics.

## Reliability and failure handling
`pipeline.py` implements retrieve -> validate -> transform/model -> metric output. It stops on missing source files or schema-level failures, quarantines row-level errors, logs counts and KPI output, preserves raw inputs, and writes a quality report. A next step is to compare synthetic patterns against real cafeteria exports and add freshness/volume alerts.
