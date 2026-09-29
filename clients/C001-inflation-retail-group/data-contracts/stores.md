# Stores Data Contract

**Client:** Inflation Retail Group (`C001`)  
**Dataset:** Stores  
**Contract status:** Draft for `P001-retail-sales`  
**Ownership:** Client-level shared reference data contract

## Purpose

This contract defines the canonical store reference data used across Inflation Retail Group engagements.

The dataset provides the business identity and descriptive attributes required to interpret transactional records such as retail sales.

## Grain

One row represents **one retail store**.

Each valid `store_id` must therefore appear at most once in the canonical store dataset.

## Business Key

The business key is:

```text
store_id
```

`store_id` uniquely identifies one retail location across the client data estate.

## Fields

| Field | Type | Nullable | Key / Role | Business Definition |
| --- | --- | ---: | --- | --- |
| `store_id` | string | No | Business key | Stable identifier of the retail store. |
| `store_name` | string | No | Descriptor | Human-readable store name. |
| `city` | string | No | Geography | City in which the store operates. |
| `province` | string | No | Geography | Canadian province or territory associated with the store. |
| `active` | boolean | No | Lifecycle state | Whether the store is currently active for normal business operations. |

## Validation Rules

### Required fields

The following fields must be present and non-null:

```text
store_id
store_name
city
province
active
```

### Store identifier

`store_id` must:

- be non-blank;
- uniquely identify one store; and
- remain stable for the lifetime of that store.

A previously assigned `store_id` must not be reused for a different store.

### Store name

`store_name` must be non-blank after trimming surrounding whitespace.

Store names are descriptive attributes and are **not** keys. Two stores may have similar names as long as their `store_id` values are distinct.

### City

`city` must be non-blank.

The city value is descriptive geography used for reporting and enrichment. It is not part of the business key.

### Province

`province` must be a valid Canadian province or territory code.

Permitted values:

```text
AB
BC
MB
NB
NL
NS
NT
NU
ON
PE
QC
SK
YT
```

### Active flag

`active` must be a valid boolean value.

```text
true
false
```

An inactive store remains a valid historical reference.

Historical sales must continue to resolve to an inactive store rather than treating that store as unknown.

## Uniqueness

`store_id` must be unique across the canonical dataset.

If more than one canonical row shares the same `store_id`, the store reference data is invalid and must not be treated as trusted.

## Referential Use

Transactional datasets may reference:

```text
sales.store_id
    -> stores.store_id
```

A sales record referencing an unknown `store_id` violates referential integrity.

The consuming engagement must surface such records as data-quality failures rather than silently dropping the store relationship or inventing a replacement store.

## Lifecycle Semantics

Store deactivation does not delete the store's identity.

When:

```text
active = false
```

the store is no longer considered active for current operations, but its historical attributes remain available for analytical interpretation of prior transactions.

This contract does not define effective-dated historical tracking of store attributes. If the business later requires historical changes such as relocations, renaming, or regional reassignment to be preserved over time, that requirement should be introduced explicitly.

## Change Expectations

The following changes are permitted without changing the business key:

- `store_name` correction or rename;
- `city` correction;
- `province` correction; and
- `active` status change.

Consumers should not assume that descriptive attributes are immutable.

The source of truth is the current canonical store reference dataset unless a later contract introduces historical versioning.

## Contract Boundary

This contract defines:

- the grain of store reference data;
- the store business key;
- required descriptive fields;
- geographic validation expectations;
- store lifecycle semantics;
- uniqueness expectations; and
- how transactional data references stores.

It does **not** define:

- source file names;
- storage formats;
- ingestion paths;
- PySpark schemas;
- warehouse dimensions;
- surrogate keys;
- slowly changing dimension strategy;
- partitioning;
- orchestration; or
- cloud infrastructure.

Those decisions belong to the consuming engagement or implementation layer.

## P001 Usage

`P001-retail-sales` will use this contract to:

- validate `sales.store_id`;
- enrich trusted sales records with store attributes;
- aggregate sales by store;
- support geographic reporting by city and province; and
- distinguish valid historical stores from unknown store references.

With `sales.md`, `products.md`, and `stores.md` defined, the minimum source contracts required for `P001-retail-sales` are now in place.
