# P001 — Retail Sales

**Client:** Inflation Retail Group (`C001`)  
**Engagement ID:** `P001`  
**Status:** Active  
**Implementation stage:** Local core pipeline and safe Parquet publication implemented; analytics/performance/cloud work remains

## Purpose

Build a trustworthy analytical foundation for Inflation Retail Group's retail sales data.

This engagement turns daily retail sales deliveries into deterministic, analysis-ready current state while explicitly handling invalid records, duplicate deliveries, corrections, stale versions, late-arriving records, unresolved source ambiguity, and safe publication.

## Business Problem

Inflation Retail Group receives sales activity from its retail operations, but operational sales records are not automatically suitable for analytical reporting.

Daily data may include:

- new transactions;
- repeated records;
- corrected transaction records;
- stale versions;
- invalid or incomplete values;
- references to unknown products or stores;
- conflicting latest versions; and
- records that arrive after the date on which the sale occurred.

Without a controlled data pipeline, these conditions can produce inconsistent revenue, order, unit, and margin figures across reports.

## Business Objective

Provide a dependable sales dataset that supports consistent reporting and analysis without requiring analysts to repeatedly clean or reconcile raw transactional files themselves.

The intended analytical use cases include:

1. revenue by business date;
2. sales comparisons across stores;
3. product and category performance;
4. units and order counts;
5. average order value;
6. gross-margin analysis;
7. investigation of rejected or ambiguous records; and
8. deterministic handling of corrections, replays, and late-arriving sales.

The current local implementation establishes the trusted processing foundation. Final warehouse modeling, reference-data enrichment, analytical SQL, and GCP deployment are still to be completed.

## Source Contracts

P001 consumes the client-level contracts under:

```text
clients/C001-inflation-retail-group/data-contracts/
├── sales.md
├── products.md
└── stores.md
```

The key sales semantics are:

```text
business key:
    (order_id, line_id)

version precedence:
    updated_at

permitted statuses:
    COMPLETED
    CANCELLED
    RETURNED
```

The client owns canonical source/reference data. P001 owns its processing logic, incremental state, quarantine outputs, curated outputs, tests, and publication behavior.

## Current Pipeline

The local pipeline currently implements:

```text
raw products/stores
        ↓
standardization
        ↓
reference validation
    ├── invalid non-duplicate rows → reference quarantine
    └── duplicate reference IDs   → fail run

raw sales
        ↓
ingestion with explicit raw schema
        ↓
standardization
        ↓
sales validation + referential integrity
    ├── invalid rows → validation quarantine
    └── accepted rows
             ↓
previous resolved state
+ previous ambiguous state
+ accepted incoming rows
             ↓
version resolution
    ├── authoritative rows → resolved state
    └── conflicting latest rows → persistent ambiguous state
             ↓
business transformations
             ↓
reconciliation
             ↓
publication-ready candidate
             ↓
local staged Parquet snapshot
             ↓
immutable run snapshot
             ↓
atomic CURRENT pointer update
```

## Implemented Correctness Guarantees

### Validation and quarantine

Sales are checked for required fields, type/parse failures, valid status and quantity semantics, monetary rules, and product/store referential integrity.

Invalid sales rows are retained in validation quarantine rather than silently dropped.

Reference-data rows are validated separately. Invalid non-duplicate reference rows are quarantined. Duplicate `product_id` or `store_id` values fail the run because reference identity is no longer deterministic.

### Duplicate and version resolution

For `(order_id, line_id)`:

- canonically identical redeliveries collapse;
- a newer valid `updated_at` supersedes an older version;
- a stale incoming version cannot replace newer trusted state;
- conflicting canonical rows at the same latest `updated_at` are not resolved arbitrarily.

### Persistent ambiguity

An ambiguous latest version is excluded from trusted current state and retained as unresolved incremental state.

That ambiguous state is supplied to future runs so the conflict cannot be accidentally forgotten merely because only one side is redelivered later.

A genuinely newer authoritative version can supersede and clear the prior ambiguity.

### Idempotency

Reprocessing the same harmless delivery does not create additional logical sales rows or change trusted business state solely because the batch was replayed.

### Derived measures

For `COMPLETED` and `RETURNED` rows:

```text
gross_sales  = quantity * unit_price
net_sales    = gross_sales - discount_amount
gross_margin = net_sales - (quantity * unit_cost)
```

For `CANCELLED` rows:

```text
gross_sales  = 0.00
net_sales    = 0.00
gross_margin = 0.00
```

### Reconciliation

Before a candidate is eligible for publication, P001 verifies invariants including:

- validated incoming count reconciles to accepted plus validation-quarantined rows;
- no duplicate curated business key exists;
- rejected rows do not leak into the candidate;
- ambiguous business keys do not leak into the candidate;
- transformed business keys match the resolved-state key set; and
- derived sales metrics independently recompute to the expected values.

### Safe local publication

Successful runs are written as immutable local Parquet snapshots.

The authoritative run is selected by a small `CURRENT` pointer. A new run becomes current only after all output and quarantine datasets are written and promoted successfully.

If a write fails before the pointer update, the previous `CURRENT` run remains authoritative.

## Current State Model

P001 deliberately separates two forms of trusted state:

### `resolved_state`

The canonical current business state used as input to the next incremental run.

It does not depend on analytical metric columns for version resolution.

### `curated_sales`

The transformed and reconciled analytical candidate produced from `resolved_state`.

This is the publication-facing dataset for the current local implementation.

Unresolved ambiguity is stored separately because it must remain outside trusted analytical data while still participating in future version resolution.

## Local Publication Layout

Development publication uses:

```text
data/dev/
├── output/
│   ├── CURRENT
│   ├── _staging/
│   └── runs/
│       └── <run_id>/
│           ├── resolved_state/
│           └── curated_sales/
│
└── quarantine/
    ├── _staging/
    └── runs/
        └── <run_id>/
            ├── ambiguous_state/
            ├── sales_validation/
            ├── products/
            └── stores/
```

`CURRENT` contains the ID of the authoritative successful run.

## Implementation Modules

```text
src/p001_retail_sales/
├── schemas.py
├── ingestion.py
├── standardization.py
├── validation.py
├── resolution.py
├── incremental.py
├── transformations.py
├── reconciliation.py
├── pipeline.py
├── publication.py
└── job.py
```

The responsibilities are intentionally separated:

- `schemas.py` — explicit raw/canonical schema definitions;
- `ingestion.py` — raw CSV reads;
- `standardization.py` — canonical parsing/normalization while preserving raw values;
- `validation.py` — row, reference, and referential-integrity validation;
- `resolution.py` — duplicate and latest-version resolution;
- `incremental.py` — combines prior trusted/ambiguous state with the incoming accepted batch;
- `transformations.py` — analytical metric derivation;
- `reconciliation.py` — pre-publication invariant checks;
- `pipeline.py` — in-memory end-to-end orchestration;
- `publication.py` — local Parquet persistence and safe publication; and
- `job.py` — loads prior published state, runs the pipeline, and publishes the successful result.

## Testing

The test suite covers the pipeline at multiple levels, including:

- ingestion;
- standardization;
- validation and quarantine;
- reference-data integrity;
- duplicate/version resolution;
- stale and corrected versions;
- same-timestamp ambiguity;
- ambiguity persistence across runs;
- idempotent replay;
- sales transformations;
- reconciliation failures;
- end-to-end pipeline behavior;
- Parquet publication and state reload; and
- multi-run persistent batch-job behavior.

Run the local suite from the engagement directory:

```powershell
pytest tests -v
```

## Environment Model

P001 keeps separate configuration scaffolds for:

```text
dev
test
prod
```

under:

```text
config/
├── base.yaml
├── dev.yaml
├── test.yaml
└── prod.yaml
```

The current local batch-job API receives paths and `run_id` explicitly. Automatic configuration loading/merging is not yet wired into the runtime.

Production cloud locations and the BigQuery dataset remain intentionally unresolved.

## Current Repository Structure

```text
P001-retail-sales/
├── README.md
├── ARCHITECTURE.md
├── pyproject.toml
├── config/
│   ├── base.yaml
│   ├── dev.yaml
│   ├── test.yaml
│   └── prod.yaml
├── data/
│   └── test/
│       └── input/
├── src/
│   └── p001_retail_sales/
│       ├── __init__.py
│       ├── schemas.py
│       ├── ingestion.py
│       ├── standardization.py
│       ├── validation.py
│       ├── resolution.py
│       ├── incremental.py
│       ├── transformations.py
│       ├── reconciliation.py
│       ├── pipeline.py
│       ├── publication.py
│       └── job.py
├── sql/
│   └── analytics.sql
└── tests/
    ├── conftest.py
    ├── test_ingestion.py
    ├── test_standardization.py
    ├── test_validation.py
    ├── test_resolution.py
    ├── test_transformations.py
    ├── test_incremental.py
    ├── test_reconciliation.py
    ├── test_pipeline.py
    ├── test_publication.py
    └── test_job.py
```

Generated `dev`, `prod`, test-output, quarantine, staging, and Parquet data are local artifacts and should not be committed.

## What Is Not Implemented Yet

The following are intentionally still ahead:

- a command-line or scheduled runner;
- automatic `base.yaml + <environment>.yaml` config resolution;
- curated product/store attribute enrichment;
- final dimensional/warehouse schema;
- analytical SQL beyond the placeholder;
- deliberate partitioning and Spark performance experiments;
- large-scale benchmark data;
- GCS persistence;
- Dataproc execution;
- BigQuery publication;
- cloud observability; and
- orchestration.

These should be added only after the local correctness and persistence foundation remains stable.

## Next Step

Run the persistent job against the real local `data/dev/` paths, inspect the produced Parquet snapshots and quarantine outputs, then proceed into analytical modeling and performance/scalability work before mapping the proven design onto GCP.
