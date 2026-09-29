# Inflation Retail Group

**Client ID:** `C001`  
**Industry:** Retail  
**Status:** Simulated  
**Primary Cloud:** Google Cloud Platform (GCP)

Inflation Retail Group is the first fictional client represented within God's Freedom Ark.

It models a large omnichannel retailer with multiple business functions that require independent but related data solutions. The client is intentionally broad enough to support projects involving retail sales, inventory, suppliers, e-commerce, finance, analytics, and future data science use cases.

## Client Data Ownership

Inflation Retail Group owns its canonical source and shared data.

Client-level data may include:

- raw source data
- shared reference and master data
- standardized reusable datasets
- quarantined client-level records
- versioned data contracts

Individual engagements may consume authorized client data, but they own their own:

- implementation code
- project-specific processing state
- intermediate data
- curated outputs
- tests
- orchestration
- operational documentation

Engagements should not depend on another engagement's internal implementation.

## Planned Canonical Data Estate

The initial logical client data estate is:

```text
Inflation Retail Group
│
├── raw/
│   ├── pos/
│   ├── suppliers/
│   ├── inventory/
│   ├── ecommerce/
│   └── finance/
│
├── reference/
│   ├── products/
│   ├── stores/
│   └── calendar/
│
├── standardized/
│
└── quarantine/
```

Large datasets are stored outside Git, primarily in GCS for the initial projects.

This repository stores only small samples, schemas, contracts, generators, configuration examples, code, tests, and documentation.

## Initial Engagements

The first planned engagements are:

| ID | Engagement | Primary Focus |
|---|---|---|
| `P001` | Retail Sales Incremental Batch Pipeline | PySpark, Dataproc, GCS, BigQuery, SQL, data quality, incremental processing |
| `P002` | Supplier Integration Pipeline | Python, Airflow/Composer, GCS, BigQuery, IAM, retries, backfills |
| `P003` | Inventory Snapshot Pipeline | PySpark, BigQuery, advanced SQL, snapshot modeling, reconciliation |

These engagements are designed to remain independently runnable and testable while sharing approved client-level data through explicit data contracts.

## Naming

This client's permanent identifier is:

```text
C001
```

Its repository directory is:

```text
C001-inflation-retail-group
```

The identifier should remain stable even if the human-readable client name changes later.
