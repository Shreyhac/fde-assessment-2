# Workflow and data model

```mermaid
flowchart LR
  A[Order placed] --> B[Accepted]
  B --> C[Prepared / ready]
  C --> D[Handed over]
  B -. stockout or payment issue .-> E[Intervention]
  C -. kitchen rework .-> E
  E --> C
  B --> F[Cancelled outcome]
  D --> G[Completed outcome]
```

```mermaid
erDiagram
  SHIFT ||--o{ ORDER : schedules
  SHIFT ||--o{ SHIFT_STAFFING : staffed_by
  ORDER ||--o{ ORDER_EVENT : has
  ORDER ||--o{ INTERVENTION : may_have
  ORDER ||--|| OUTCOME : ends_in
  SHIFT {
    string shift PK
    int capacity_orders_per_15m
  }
  SHIFT_STAFFING {
    date roster_date PK
    string shift PK
    int staff_scheduled
    int staff_present
  }
  ORDER {
    string order_id PK
    string shift FK
    string channel
    int item_count
  }
  ORDER_EVENT {
    string order_id FK
    string event_type
    datetime event_ts
  }
  INTERVENTION {
    string order_id FK
    string issue_code
  }
  OUTCOME {
    string order_id FK
    boolean cancelled
    number wait_minutes
    boolean within_target
  }
```

The SQLite artifact (`workflow.db`) has three tables: `orders` (one clean row per order, with stage durations), `shifts` (from `shift_config.json`) and `shift_staffing` (queried from the roster database). Shift metrics join `orders` to `shift_staffing` on (`order_date`, `shift`) to get actual staff-hours, orders per staff-hour and the short-staffed vs fully staffed comparison. Events, interventions and outcomes are stored as columns on the order row because each is one-to-one with an order in this MVP.
