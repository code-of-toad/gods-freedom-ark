# Data Contracts

This directory contains the versioned data contracts for shared Inflation Retail Group datasets.

A data contract defines the expected structure and meaning of a dataset that may be consumed by one or more engagements.

Each contract should document, where relevant:

- dataset name
- contract version
- owning source system
- business meaning
- grain
- business or primary key
- field names and data types
- nullability
- domain or range rules
- delivery cadence
- freshness expectations
- partitioning expectations
- downstream consumers
- compatibility notes

## Naming Convention

Use:

```text
<dataset-name>-v<version>.yaml
```

Examples:

```text
pos-sales-v1.yaml
product-master-v1.yaml
inventory-snapshot-v1.yaml
```

## Versioning Rule

Do not silently redefine an existing contract when a breaking schema or semantic change occurs.

Instead, introduce a new version:

```text
pos-sales-v1.yaml
pos-sales-v2.yaml
```

Backward-compatible clarifications may be documented within the existing version when they do not change the contract expected by consumers.

## Ownership Principle

Shared client datasets are owned at the client level.

Engagements consume these datasets through their contracts rather than depending on another engagement's internal implementation.
