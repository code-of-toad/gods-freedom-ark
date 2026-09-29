# Engagements

This directory contains the independent client engagements performed for Inflation Retail Group.

Each engagement represents a distinct business problem, technical solution, and delivery boundary.

## Naming Convention

Use:

```text
P###-<engagement-slug>
```

Examples:

```text
P001-retail-sales
P002-supplier-integration
P003-inventory-snapshot
```

The `P###` identifier is permanent. The human-readable engagement name may change without changing the identifier.

## Independence Principle

Each engagement should remain independently understandable, runnable, testable, and deployable.

An engagement owns its own:

- implementation code
- project-specific configuration
- orchestration
- tests
- intermediate processing state
- curated outputs
- operational documentation
- architecture decisions
- incident records

Engagements must not import another engagement's internal implementation.

Shared client data should be consumed through explicit client-level data contracts.

Reusable code should remain local to an engagement until genuine reuse justifies promotion into a shared package.

## Initial Planned Engagements

| ID | Engagement | Primary Focus |
|---|---|---|
| `P001` | Retail Sales Incremental Batch Pipeline | PySpark, Dataproc, GCS, BigQuery, SQL, data quality, incremental processing |
| `P002` | Supplier Integration Pipeline | Python, Airflow/Composer, GCS, BigQuery, IAM, retries, backfills |
| `P003` | Inventory Snapshot Pipeline | PySpark, BigQuery, advanced SQL, snapshot modeling, reconciliation |

These projects are designed so that their combined technology coverage supports practical data engineering job readiness while preserving independent project boundaries.
