# Demo outline (about 4.5 minutes)

1. **Problem and stakeholders (45 sec):** Explain the order workflow, why unpredictable waits matter, and who owns the decision.
2. **Raw sources (45 sec):** Show the three retrieval types: `orders.csv` (file export), `shift_config.json` (configuration) and the SQL query on `staff_roster.db`. Point out the manifest checks and that raw sources are never changed.
3. **Validation and model (45 sec):** Run `python3 pipeline.py`; show duplicate/missing rows quarantined in `quality_report.json` and the SQLite order model.
4. **Metrics (60 sec):** Open `summary.json`; state the 12-minute KPI (57.55%), the prep-stage share of the wait, and the short-staffed vs fully staffed gap (50.89% vs 59.54%).
5. **Judgement call: no imputation (30 sec):** A completed order with no handover time is quarantined, not guessed. Imputing it would silently change the KPI denominator, and the manager should see the data gap.
6. **Reliability and next steps (45 sec):** Show `pipeline.log`, explain failure handling and assumptions, then name the unresolved questions to validate with cafeteria staff and how real exports would replace synthetic inputs.
