# Sales Data Contract

**Client:** Inflation Retail Group (`C001`)  
**Dataset:** Sales  
**Contract status:** Draft for `P001-retail-sales`  
**Ownership:** Client-level canonical data contract

## Purpose

This contract defines the structure and business expectations of canonical sales records made available by Inflation Retail Group.

The contract is intentionally independent of any specific processing framework, storage format, or warehouse technology.

## Grain

One row represents **one version of one retail order line**.

A single customer order may therefore contain multiple rows when it contains multiple purchased products or line items.

The same business order line may also appear more than once across source deliveries when:

- a record is redelivered;
- a previously delivered value is corrected; or
- a later version supersedes an earlier version.

## Business Key

The business key is:

```text
(order_id, line_id)
```

Together, these fields identify one logical order line.

They are not sufficient by themselves to identify a specific delivered version of that order line.

## Versioning

`updated_at` identifies when a source-system version of an order line was last updated.

When multiple valid records exist for the same business key, downstream processing may use `updated_at` to determine the most recent version.

If two conflicting records share the same business key and the same `updated_at`, the contract does not define which record wins. Such ambiguity must be surfaced as a data-quality issue rather than resolved arbitrarily.

## Fields

| Field | Type | Nullable | Key / Role | Business Definition |
| --- | --- | ---: | --- | --- |
| `order_id` | string | No | Business key | Identifier of the retail order. |
| `line_id` | integer | No | Business key | Identifier of the line within the order. |
| `sale_date` | date | No | Business date | Date on which the sale occurred. |
| `store_id` | string | No | Foreign key | Store responsible for the sale. |
| `product_id` | string | No | Foreign key | Product sold on this order line. |
| `quantity` | integer | No | Measure input | Number of units associated with the order line. |
| `unit_price` | decimal(12,2) | No | Measure input | Selling price per unit before line-level discount. |
| `unit_cost` | decimal(12,2) | No | Measure input | Cost per unit used for gross-margin analysis. |
| `discount_amount` | decimal(12,2) | No | Measure input | Total discount applied to the order line. |
| `order_status` | string | No | Business state | Current source-system status of the order line. |
| `updated_at` | timestamp | No | Versioning | Timestamp of the source-system version represented by the row. |

## Permitted Order Statuses

The canonical contract recognizes:

```text
COMPLETED
CANCELLED
RETURNED
```

Additional statuses require an explicit contract revision before they are treated as valid canonical values.

## Relationships

### Store

```text
sales.store_id
    -> stores.store_id
```

Every valid sales record must reference a known store.

### Product

```text
sales.product_id
    -> products.product_id
```

Every valid sales record must reference a known product.

## Validation Rules

### Required fields

The following fields must be present and non-null:

```text
order_id
line_id
sale_date
store_id
product_id
quantity
unit_price
unit_cost
discount_amount
order_status
updated_at
```

### Business-key validity

`order_id` must be non-blank.

`line_id` must be a positive integer.

The combination:

```text
(order_id, line_id)
```

must identify one logical order line.

Multiple delivered versions of the same business key are permitted only when they represent repeated or updated source records.

### Quantity

For `COMPLETED` sales:

```text
quantity > 0
```

For `RETURNED` records:

```text
quantity < 0
```

`quantity = 0` is invalid.

### Monetary values

The following values must not be negative:

```text
unit_price
unit_cost
discount_amount
```

The line's gross sales value is:

```text
quantity * unit_price
```

For completed sales, the discount must not exceed the gross sales amount:

```text
discount_amount <= quantity * unit_price
```

### Order status

`order_status` must belong to the permitted status domain defined by this contract.

### Dates and timestamps

`sale_date` represents the business date and may be earlier than the delivery or processing date.

This permits legitimate late-arriving sales.

`updated_at` must be a valid timestamp and is used for source-version ordering.

A later delivery is not automatically a newer business version; version precedence is based on `updated_at`, not file-arrival order alone.

### Referential integrity

`store_id` must exist in the canonical store reference dataset.

`product_id` must exist in the canonical product reference dataset.

Unknown references are data-quality failures and must not silently enter trusted analytical outputs.

## Duplicate and Correction Semantics

The contract distinguishes the following cases.

### Exact redelivery

Two rows are exact duplicates when all canonical fields are identical.

Exact redelivery must not result in double-counting.

### Repeated business key with newer version

If two valid rows share:

```text
(order_id, line_id)
```

and one has a later `updated_at`, the later source version represents the current state of that logical order line.

### Repeated business key with stale version

A delivered row with an older `updated_at` than the currently known version is stale and must not replace the newer state.

### Ambiguous conflict

If conflicting rows share the same business key and the same `updated_at`, the source data is ambiguous.

The downstream system must surface this condition for investigation rather than choosing a winner nondeterministically.

## Derived Measures

The source contract provides inputs from which downstream analytical measures may be derived.

For `COMPLETED` and `RETURNED` records, the analytical measures are:

### Gross sales

```text
gross_sales = quantity * unit_price
```

### Net sales

```text
net_sales = gross_sales - discount_amount
```

### Gross margin

```text
gross_margin = net_sales - (quantity * unit_cost)
```

For `RETURNED` records, `quantity` is negative, so the same formulas naturally reverse the sale and its associated margin contribution.

For `CANCELLED` records, the analytical measures must contribute zero:

```text
gross_sales = 0.00
net_sales = 0.00
gross_margin = 0.00
```

Cancelled rows may remain part of the trusted current-state dataset as valid business-state records, but they must not contribute revenue, net sales, or gross margin to downstream analytical aggregations.

These are derived analytical measures and do not need to exist as canonical source fields.

## Late-Arriving Data

A record is late-arriving when it is delivered after its `sale_date`.

Late arrival is not, by itself, a data-quality failure.

Downstream processing must be capable of incorporating a valid late-arriving record into the correct business date without double-counting previously processed data.

## Reprocessing Expectations

A downstream consumer must be able to process the same source delivery more than once without changing the final trusted state solely because the delivery was replayed.

This contract therefore assumes that consumers will preserve enough identity and version information to support deterministic, idempotent processing.

## Source Metadata

Delivery-specific metadata such as the following may be attached during ingestion:

```text
source_file
source_row_id
batch_id
run_id
ingested_at
```

These are operational metadata fields rather than canonical business fields.

They may be required by an engagement for traceability, but they do not change the business meaning or grain of the canonical sales record.

## Contract Boundary

This contract defines:

- what one sales record represents;
- which business fields exist;
- how logical records are identified;
- how versions are ordered;
- which values are considered valid; and
- how sales records relate to shared store and product reference data.

It does **not** define:

- file naming conventions;
- storage paths;
- PySpark schemas;
- transformation functions;
- warehouse tables;
- partitioning strategies;
- orchestration;
- cloud services; or
- reporting-layer implementation.

Those decisions belong to the consuming engagement or platform implementation.

## P001 Usage

`P001-retail-sales` uses this contract to implement:

- ingestion and canonical standardization;
- sales validation;
- duplicate/redelivery handling;
- correction and stale-version handling;
- persistent ambiguous-version handling;
- late-arriving-data handling;
- incremental and idempotent processing;
- reconciliation;
- derived sales measures; and
- trusted local analytical sales state.

With `sales.md`, `products.md`, and `stores.md` defined, the minimum source contracts required for the current P001 pipeline are in place.
