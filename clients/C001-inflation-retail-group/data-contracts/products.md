# Products Data Contract

**Client:** Inflation Retail Group (`C001`)  
**Dataset:** Products  
**Contract status:** Draft for `P001-retail-sales`  
**Ownership:** Client-level shared reference data contract

## Purpose

This contract defines the canonical product reference data used across Inflation Retail Group engagements.

The dataset provides the business identity and descriptive attributes required to interpret transactional records such as retail sales.

## Grain

One row represents **one product**.

Each valid `product_id` must therefore appear at most once in the canonical product dataset.

## Business Key

The business key is:

```text
product_id
```

`product_id` uniquely identifies one product across the client data estate.

## Fields

| Field | Type | Nullable | Key / Role | Business Definition |
| --- | --- | ---: | --- | --- |
| `product_id` | string | No | Business key | Stable identifier of the product. |
| `product_name` | string | No | Descriptor | Human-readable product name. |
| `category` | string | No | Classification | Business category to which the product belongs. |
| `active` | boolean | No | Lifecycle state | Whether the product is currently active for normal business use. |

## Validation Rules

### Required fields

The following fields must be present and non-null:

```text
product_id
product_name
category
active
```

### Product identifier

`product_id` must:

- be non-blank;
- uniquely identify one product; and
- remain stable for the lifetime of that product.

A previously assigned `product_id` must not be reused for a different product.

### Product name

`product_name` must be non-blank after trimming surrounding whitespace.

Product names are descriptive attributes and are **not** keys. Multiple products may have similar or identical display names if their `product_id` values are distinct.

### Category

`category` must be non-blank.

Categories are controlled business classifications used for grouping and reporting.

A new category may be introduced without changing the grain or business key of this contract, but consumers must not assume that the category domain is permanently fixed unless a separate controlled taxonomy is defined.

### Active flag

`active` must be a valid boolean value.

```text
true
false
```

An inactive product remains a valid historical reference.

Historical sales must continue to resolve to an inactive product rather than treating that product as unknown.

## Uniqueness

`product_id` must be unique across the canonical dataset.

If more than one canonical row shares the same `product_id`, the product reference data is invalid and must not be treated as trusted.

## Referential Use

Transactional datasets may reference:

```text
sales.product_id
    -> products.product_id
```

A sales record referencing an unknown `product_id` violates referential integrity.

The consuming engagement must surface such records as data-quality failures rather than silently dropping the product relationship or inventing a replacement product.

## Lifecycle Semantics

Product deactivation does not delete the product's identity.

When:

```text
active = false
```

the product is no longer considered active for current business use, but its historical attributes remain available for analytical interpretation of prior transactions.

This contract does not define effective-dated historical attribute tracking. If the business later requires historical product-name or category changes to be preserved over time, that requirement should be introduced explicitly rather than inferred.

## Change Expectations

The following changes are permitted without changing the business key:

- `product_name` correction;
- `category` reassignment; and
- `active` status change.

Consumers should not assume that descriptive attributes are immutable.

The source of truth is the current canonical product reference dataset unless a later contract introduces historical versioning.

## Contract Boundary

This contract defines:

- the grain of product reference data;
- the product business key;
- required descriptive fields;
- product lifecycle semantics;
- uniqueness expectations; and
- how transactional data references products.

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

`P001-retail-sales` currently uses this contract to:

- validate canonical product reference rows;
- enforce `sales.product_id` referential integrity;
- quarantine invalid product-reference rows; and
- distinguish valid inactive historical products from unknown product references.

Future analytical modeling will also use trusted product attributes for product/category enrichment and aggregation.
