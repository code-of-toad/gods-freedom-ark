# P001 Test Fixtures

These fixtures are intentionally small and deterministic.

They exercise ingestion, parsing, validation, referential integrity, incremental processing, idempotency, corrections, stale versions, late arrivals, and reconciliation.

## Reference data

- `products.csv`: clean product reference data.
- `stores.csv`: clean store reference data.
- Inactive reference rows remain valid historical references.

## Sales deliveries

### Day 1 — `sales_2026-10-01.csv`

- valid multi-line order;
- exact duplicate;
- unknown product;
- unknown store;
- zero quantity;
- negative monetary value;
- invalid numeric text.

### Day 2 — `sales_2026-10-02.csv`

- new valid sale;
- newer correction;
- stale version;
- invalid order status;
- valid return;
- late-arriving sale;
- discount greater than gross sales.

### Day 3 — `sales_2026-10-03.csv`

- new valid sale;
- exact replay of a prior delivery;
- same-key/same-timestamp conflicting versions;
- another correction;
- another late-arriving sale;
- missing required business key;
- invalid timestamp text.

The filename date represents simulated delivery date. `sale_date` is the business date. `updated_at` determines version precedence.
