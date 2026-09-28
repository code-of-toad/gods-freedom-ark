# God's Freedom Ark

A multi-client data engineering, analytics, and data science portfolio platform for building, operating, benchmarking, and documenting production-oriented data systems.

> **Current priority:** become job-ready for a data engineering role by building realistic systems with Python, SQL, PySpark, GCP, orchestration, warehousing, data quality, observability, CI/CD, and production troubleshooting.
>
> **Current documentation model:** this root README intentionally duplicates the repository's important conventions so that it can serve as a single training-wheel reference. As the repository matures, detailed rules will move into `standards/` and reusable structures into `templates/`. Those documents will eventually become authoritative.

---

## Table of Contents

1. [Purpose](#1-purpose)
2. [Repository Operating Model](#2-repository-operating-model)
3. [Governing Principles](#3-governing-principles)
4. [Root Repository Structure](#4-root-repository-structure)
5. [Identifiers and Naming](#5-identifiers-and-naming)
6. [Client Structure](#6-client-structure)
7. [Client Metadata](#7-client-metadata)
8. [Data Contracts](#8-data-contracts)
9. [Engagement Structure](#9-engagement-structure)
10. [Engagement Metadata](#10-engagement-metadata)
11. [Project Independence and Shared Code](#11-project-independence-and-shared-code)
12. [Environment and Configuration Model](#12-environment-and-configuration-model)
13. [Secrets and Credentials](#13-secrets-and-credentials)
14. [Cloud Resource Naming and Labels](#14-cloud-resource-naming-and-labels)
15. [Data Storage Model](#15-data-storage-model)
16. [Data Lifecycle and Retention](#16-data-lifecycle-and-retention)
17. [Large-Scale Data and Benchmarking](#17-large-scale-data-and-benchmarking)
18. [Operational Metadata and Observability](#18-operational-metadata-and-observability)
19. [Testing Model](#19-testing-model)
20. [Data Quality Model](#20-data-quality-model)
21. [Git Workflow](#21-git-workflow)
22. [CI/CD Expectations](#22-cicd-expectations)
23. [Architecture Decision Records](#23-architecture-decision-records)
24. [Incident Documentation](#24-incident-documentation)
25. [Dependency Management](#25-dependency-management)
26. [Public Repository Hygiene](#26-public-repository-hygiene)
27. [Initial Walmart-Readiness Engagements](#27-initial-walmart-readiness-engagements)
28. [Technology Coverage](#28-technology-coverage)
29. [Explicitly Deferred Architecture](#29-explicitly-deferred-architecture)
30. [Future Evolution](#30-future-evolution)
31. [Consistency Checklist for Every New Engagement](#31-consistency-checklist-for-every-new-engagement)
32. [Copyright](#32-copyright)

---

# 1. Purpose

**God's Freedom Ark (GFA)** simulates a data consulting firm serving multiple clients and managing multiple independent engagements.

The repository has three immediate purposes.

### Job readiness

Build practical proficiency with:

- Python
- SQL
- PySpark
- Google Cloud Storage
- Dataproc
- BigQuery
- Airflow / Cloud Composer
- IAM and service accounts
- Cloud Logging and Cloud Monitoring
- Git / GitHub
- CI/CD
- dimensional and analytical modeling
- data quality
- incremental processing
- production debugging
- performance engineering

### Portfolio

Each substantial engagement should become a self-contained case study covering:

- business problem
- requirements
- architecture
- source systems
- data contracts
- transformations
- orchestration
- testing
- security
- observability
- performance
- failure scenarios
- design trade-offs
- operational runbooks
- results

### Long-term extensibility

The repository must remain capable of hosting future DE, DA, and DS projects using unrelated technologies without forcing them into one runtime or cloud.

[Back to top](#gods-freedom-ark)

---

# 2. Repository Operating Model

The conceptual hierarchy is:

```text
God's Freedom Ark
        │
        ├── Client C001
        │      │
        │      ├── Canonical Client Data Estate
        │      │
        │      ├── Engagement P001
        │      ├── Engagement P002
        │      └── Engagement P003
        │
        └── Client C002
               └── Future Engagements
```

The ownership model is:

> **Clients own canonical source data.**  
> **Engagements own their implementations, intermediate state, and outputs.**  
> **Engagements consume shared data through explicit contracts.**  
> **One engagement must not depend on another engagement's internal implementation.**

A future real firm may use one team per engagement. The repository models the engagement as the primary technical delivery boundary.

[Back to top](#gods-freedom-ark)

---

# 3. Governing Principles

These principles should remain true unless an Architecture Decision Record explicitly documents an exception.

1. **Clients own canonical data.**
2. **Engagements own implementations and outputs.**
3. **Engagements remain independently runnable and testable.**
4. **Projects communicate through data contracts, not implementation coupling.**
5. **Configuration is externalized; environment-specific values are never buried in code.**
6. **Secrets and large datasets never enter Git.**
7. **Production behavior is simulated deliberately: scale, failures, monitoring, recovery, and reruns matter.**
8. **Shared abstractions are created only after demonstrated reuse.**
9. **Meaningful architecture decisions are documented.**
10. **Immediate engineering learning value beats speculative platform engineering.**
11. **Technology choices follow business and engineering requirements rather than résumé keyword collection.**
12. **The same business logic should not be duplicated across interfaces or environments.**
13. **Human-readable names may change; stable identifiers should not.**
14. **Public repository contents must be safe to remain public permanently.**

[Back to top](#gods-freedom-ark)

---

# 4. Root Repository Structure

Initial structure:

```text
gods-freedom-ark/
│
├── README.md
├── .gitignore
├── COPYRIGHT.md
│
├── clients/
│   └── C001-<fictional-client>/
│       ├── README.md
│       ├── client.yaml
│       ├── data-contracts/
│       └── engagements/
│
├── standards/
│   ├── naming.md
│   ├── engagement-structure.md
│   ├── metadata.md
│   ├── configuration.md
│   ├── data-contracts.md
│   ├── data-quality.md
│   ├── testing.md
│   ├── security.md
│   ├── observability.md
│   ├── data-lifecycle.md
│   ├── git-workflow.md
│   └── dependency-management.md
│
├── catalog/
│   ├── clients.yaml
│   └── engagements.yaml
│
├── templates/
│   ├── client-template/
│   ├── engagement-template/
│   ├── client-template.yaml
│   ├── engagement-template.yaml
│   ├── data-contract-template.yaml
│   ├── adr-template.md
│   └── incident-template.md
│
└── .github/
    ├── workflows/
    └── pull_request_template.md
```

Do not create empty directories merely to make the tree look complete. Add optional folders when a real project needs them.

[Back to top](#gods-freedom-ark)

---

# 5. Identifiers and Naming

## Stable identifiers

Use permanent human-facing codes:

```text
C001   client
P001   engagement/project
```

Future entities may use similar stable codes:

```text
PIPE001   pipeline
DATA001   data asset
INC001    incident
ADR-001   architecture decision
```

Names may change. IDs should not.

For a future database-backed application, opaque internal IDs such as UUIDs may be added. Human-facing codes should remain useful aliases.

## Directory naming

Use:

```text
C001-northstar-retail
P001-retail-sales
```

Rules:

- lowercase slug after the stable ID
- hyphens, not spaces or underscores, in top-level directory slugs
- no dates in permanent project directory names
- no cloud-provider name in a project directory unless the cloud itself is the subject of the project

## Python naming

Use normal Python conventions:

```text
snake_case.py
PascalCase
snake_case_function()
UPPER_CASE_CONSTANT
```

## SQL naming

Default convention:

```text
snake_case
```

Example:

```text
fact_sales
dim_product
order_id
sale_date
```

## Cloud naming

Use lowercase, provider-compatible resource names based on:

```text
gfa-{client}-{project?}-{resource}-{environment}
```

Examples:

```text
gfa-c001-raw-dev
gfa-c001-reference-dev
gfa-c001-p001-curated-dev
```

Where a provider imposes different naming restrictions, preserve the same logical components.

[Back to top](#gods-freedom-ark)

---

# 6. Client Structure

A client represents an independent organization and data estate.

Example:

```text
clients/
└── C001-northstar-retail/
    ├── README.md
    ├── client.yaml
    ├── data-contracts/
    └── engagements/
        ├── P001-retail-sales/
        ├── P002-supplier-integration/
        └── P003-inventory-snapshot/
```

Client-level concerns include:

- canonical raw data
- shared reference/master data
- standardized shared datasets
- client-wide data contracts
- client-wide security boundaries
- shared data ownership

Engagement-specific concerns belong inside the engagement.

[Back to top](#gods-freedom-ark)

---

# 7. Client Metadata

Every client must have `client.yaml`.

Minimum v1 fields:

```yaml
client_id: C001
name: Northstar Retail Group
industry: retail
status: simulated

cloud:
  primary: gcp

data_estate:
  canonical_raw: true
  standardized_layer: true
  shared_reference_data: true
```

Allowed `status` values for portfolio clients:

```text
simulated
active
archived
```

Additional metadata may be added later, but existing fields should not be silently reinterpreted.

[Back to top](#gods-freedom-ark)

---

# 8. Data Contracts

Shared datasets require explicit contracts.

Store them under:

```text
clients/C001-.../data-contracts/
```

Example:

```text
pos-sales-v1.yaml
product-master-v1.yaml
inventory-snapshot-v1.yaml
```

Minimum contract information:

```yaml
name: pos_sales
version: 1

owner: retail-pos-system

grain:
  one row per order line

business_key:
  - order_id
  - line_id

fields:
  order_id:
    type: string
    nullable: false

  line_id:
    type: integer
    nullable: false

  sale_timestamp:
    type: timestamp
    nullable: false

delivery:
  cadence: daily

consumers:
  - P001
```

A contract should define, where relevant:

- name
- version
- owner
- business meaning
- grain
- business/primary key
- fields
- types
- nullability
- accepted domain/range
- delivery cadence
- freshness expectations
- partitioning expectations
- consumers
- compatibility notes

Breaking changes should produce a new contract version instead of silently changing an existing contract.

Example:

```text
pos-sales-v1.yaml
pos-sales-v2.yaml
```

[Back to top](#gods-freedom-ark)

---

# 9. Engagement Structure

A substantial DE engagement may use:

```text
P001-retail-sales/
│
├── README.md
├── engagement.yaml
│
├── architecture/
│   ├── overview.md
│   └── diagrams/
│
├── src/
│   ├── ingestion/
│   ├── validation/
│   ├── transformation/
│   └── loading/
│
├── sql/
│   ├── ddl/
│   ├── transformations/
│   ├── quality/
│   └── reconciliation/
│
├── orchestration/
│   └── dags/
│
├── config/
│   ├── dev.yaml
│   ├── test.yaml
│   └── prod.yaml
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── data_quality/
│   ├── end_to_end/
│   └── performance/
│
├── data/
│   ├── sample/
│   └── README.md
│
├── benchmarks/
│
├── docs/
│   ├── design.md
│   ├── runbook.md
│   ├── decisions/
│   └── incidents/
│
├── scripts/
│   ├── generate_data.py
│   ├── bootstrap.py
│   └── teardown.py
│
├── pyproject.toml
└── .env.example
```

## Required for every substantial engagement

At minimum:

```text
README.md
engagement.yaml
src/ or sql/ or another clearly defined implementation directory
tests/
config/ when environment-specific configuration exists
docs/ when operational/design documentation is needed
```

## Optional

Create only when justified:

```text
architecture/
orchestration/
benchmarks/
dashboards/
notebooks/
infrastructure/
Dockerfile
Terraform
```

DA and DS projects may use different internal layouts. The common contract is metadata, ownership, reproducibility, documentation, and independence—not identical folders.

[Back to top](#gods-freedom-ark)

---

# 10. Engagement Metadata

Every engagement must have `engagement.yaml`.

Minimum v1 structure:

```yaml
engagement_id: P001
client_id: C001

name: Retail Sales Incremental Pipeline
status: active

services:
  - data-engineering

patterns:
  - incremental-batch
  - etl
  - dimensional-modeling

technologies:
  languages:
    - python
    - sql

  processing:
    - pyspark
    - dataproc

  storage:
    - gcs
    - bigquery

  orchestration:
    - airflow
    - cloud-composer

  observability:
    - cloud-logging
    - cloud-monitoring

concepts:
  - idempotency
  - partitioning
  - deduplication
  - data-quality
  - late-arriving-data
```

Allowed engagement `status` values:

```text
planned
active
paused
completed
archived
```

Recommended `services` values:

```text
data-engineering
data-analytics
data-science
analytics-engineering
machine-learning
platform-engineering
```

Do not invent near-duplicate vocabulary casually. Prefer existing terms so metadata remains searchable.

[Back to top](#gods-freedom-ark)

---

# 11. Project Independence and Shared Code

An engagement must not import implementation details from another engagement.

Bad:

```python
from p002.src.helpers import clean_supplier
```

Preferred mechanisms:

### Shared data

Publish through a documented dataset/data contract.

```text
P001 ──> standardized shared dataset ──> P002
```

### Shared code

If multiple projects genuinely need the same generic code, promote it later into a shared package.

Use the **rule of three**:

> Do not extract generic shared infrastructure merely because two files look similar. Prefer demonstrated reuse across roughly three meaningful consumers before creating a shared abstraction.

Until then, modest duplication is preferable to premature coupling.

[Back to top](#gods-freedom-ark)

---

# 12. Environment and Configuration Model

Every serious project should conceptually support:

```text
dev
test
prod
```

Even if only `dev` is actually deployed initially.

Environment-specific values must not be hard-coded in application logic.

Bad:

```python
bucket = 'gs://gfa-c001-raw-prod'
```

Preferred:

```python
bucket = config.raw_bucket
```

Example:

```yaml
environment: dev

storage:
  raw_bucket: gfa-c001-raw-dev
  project_bucket: gfa-c001-p001-dev
```

## Configuration precedence

For v1, use this conceptual precedence from lowest to highest:

```text
code defaults
    ↓
shared/project config
    ↓
environment-specific config
    ↓
environment variables
    ↓
runtime parameters
```

Secrets must not be stored in ordinary YAML configuration.

A project README must document how its configuration is resolved.

[Back to top](#gods-freedom-ark)

---

# 13. Secrets and Credentials

Never commit:

```text
.env
service-account keys
API tokens
passwords
private certificates
database credentials
cloud access keys
OAuth secrets
```

Commit safe examples instead:

```text
.env.example
```

Example:

```text
API_BASE_URL=
API_KEY=
GCP_PROJECT_ID=
```

Local development may use local environment variables or authenticated developer tooling.

Cloud deployments should eventually use the cloud provider's secret-management and workload/service-account mechanisms instead of static credential files.

If a secret is accidentally committed:

1. Treat it as compromised.
2. Rotate/revoke it immediately.
3. Remove it from current files.
4. Clean Git history if appropriate.
5. Document the incident if it materially affects the project.

[Back to top](#gods-freedom-ark)

---

# 14. Cloud Resource Naming and Labels

Resource names should identify ownership.

Logical format:

```text
gfa-{client}-{project?}-{resource}-{environment}
```

Examples:

```text
gfa-c001-raw-dev
gfa-c001-reference-dev
gfa-c001-p001-curated-dev
```

Where supported, resources should carry labels/tags such as:

```text
organization = gfa
client       = c001
engagement   = p001
environment  = dev
purpose      = curated
managed_by   = manual | terraform | pipeline
```

These labels support:

- cost attribution
- cleanup
- auditing
- resource discovery
- environment separation

[Back to top](#gods-freedom-ark)

---

# 15. Data Storage Model

## Canonical client estate

Client-owned:

```text
raw/
reference/
standardized/
quarantine/
```

Example:

```text
gs://gfa-c001-raw-dev/
gs://gfa-c001-reference-dev/
gs://gfa-c001-standardized-dev/
```

## Engagement-owned estate

Project-owned:

```text
staging/
intermediate/
curated/
checkpoint/
benchmark/
scratch/
temp/
```

Example:

```text
gs://gfa-c001-p001-curated-dev/
gs://gfa-c001-p001-benchmark-dev/
```

## Rule

> Do not create an independent copy of canonical client raw data for every project merely for convenience.

Projects should read authorized canonical sources and own their downstream processing state and outputs.

[Back to top](#gods-freedom-ark)

---

# 16. Data Lifecycle and Retention

Every data class should have an explicit lifecycle.

| Class | Meaning | Default intention |
|---|---|---|
| `raw` | Canonical received source | Long-lived |
| `reference` | Master/shared reference data | Long-lived |
| `standardized` | Reusable normalized client data | Long-lived |
| `curated` | Project-published output | Long-lived while useful |
| `quarantine` | Rejected/problem records | Limited retention |
| `checkpoint` | Processing state | Pipeline-dependent |
| `benchmark` | Performance experiments | Temporary |
| `scratch` | Developer experimentation | Short-lived |
| `temp` | Processing intermediates | Very short-lived |

Projects that create large benchmark/scratch/temp datasets should define deletion or lifecycle rules.

Do not keep expensive data indefinitely simply because cleanup was forgotten.

[Back to top](#gods-freedom-ark)

---

# 17. Large-Scale Data and Benchmarking

Large benchmark datasets belong in cloud object storage, not Git.

Git should contain deterministic generators when practical.

Example:

```bash
python scripts/generate_data.py \
  --rows 100000000 \
  --stores 500 \
  --products 50000 \
  --skew 0.20 \
  --seed 42
```

The benchmark metadata should record enough information to reproduce the experiment:

```yaml
benchmark_id: B001
seed: 42
rows: 100000000
skew: 0.20
```

Useful deliberately generated pathologies include:

- severe key skew
- excessive shuffle
- very high cardinality
- small-file problems
- oversized files
- partition imbalance
- duplicate batches
- malformed records
- late-arriving data
- schema drift
- expensive joins

The objective is not merely to process the largest possible dataset.

The objective is to reproduce meaningful production bottlenecks and measure how engineering decisions change them.

[Back to top](#gods-freedom-ark)

---

# 18. Operational Metadata and Observability

Pipelines should progressively converge on common run metadata.

Recommended fields:

```text
client_id
engagement_id
pipeline_id
environment

run_id
batch_id

source
source_file

started_at
completed_at
duration

status

rows_read
rows_accepted
rows_rejected
rows_written

bytes_read
bytes_written
```

Recommended statuses:

```text
started
running
succeeded
failed
cancelled
skipped
```

Logs should be structured enough to answer:

- which client?
- which project?
- which pipeline?
- which run?
- which batch/source?
- what failed?
- at what stage?
- how much data was processed?
- how long did it take?

Avoid logs that contain only vague free-form statements with no run context.

[Back to top](#gods-freedom-ark)

---

# 19. Testing Model

Keep test purposes distinct.

```text
unit
integration
data-quality
end-to-end
reconciliation
performance
```

## Unit tests

Test isolated business/transformation logic.

## Integration tests

Test interactions between meaningful components such as Spark + storage or ingestion + parser.

## Data-quality tests

Test the data itself:

- required fields
- valid domains/ranges
- primary-key uniqueness
- referential integrity
- business rules
- schema conformity

## End-to-end tests

Validate a complete representative pipeline flow.

## Reconciliation tests

Prove that source and destination counts/measures reconcile according to business expectations.

## Performance tests

Compare runtime, shuffle, partitioning, join strategies, file layouts, or other measurable engineering behavior.

A green unit test suite does not prove the data is valid. A green DQ suite does not prove deployment or orchestration works.

[Back to top](#gods-freedom-ark)

---

# 20. Data Quality Model

Where applicable, validate:

### Structural quality

- schema
- types
- required fields
- parseability

### Key integrity

- primary-key uniqueness
- composite-key uniqueness
- referential integrity

### Business rules

Examples:

```text
quantity > 0
unit_price >= 0
discount_amount >= 0
sale_date is plausible
```

### Accepted vs rejected

Pipelines should avoid silently discarding invalid records.

Prefer:

```text
input
  ↓
validation
  ├── accepted
  └── rejected/quarantine + reason
```

Rejected rows should carry useful failure reasons where practical.

### Metrics

Track quality metrics such as:

```text
rows_received
rows_accepted
rows_rejected
rejection_rate
rule_failure_counts
```

[Back to top](#gods-freedom-ark)

---

# 21. Git Workflow

Default branch:

```text
main
```

Feature branches:

```text
feature/p001-ingestion
feature/p001-data-quality
feature/p002-airflow-dag
```

Optional bug-fix branches:

```text
fix/p001-duplicate-handling
```

Workflow:

```text
branch
  ↓
implement
  ↓
test
  ↓
commit
  ↓
push
  ↓
pull request
  ↓
review/checks
  ↓
merge
```

Even for solo work, use pull requests periodically to practice normal team engineering flow.

## Commit guidance

Prefer commits that are:

- focused
- understandable
- reversible
- appropriately scoped

Examples:

```text
Add explicit sales schema validation
Implement idempotent BigQuery merge
Add skew benchmark dataset generator
```

Avoid meaningless history such as:

```text
stuff
changes
fix
more changes
```

[Back to top](#gods-freedom-ark)

---

# 22. CI/CD Expectations

Start small.

Initial CI may perform:

```text
lint
unit tests
metadata validation
```

As projects mature:

```text
commit
   ↓
lint
   ↓
unit tests
   ↓
integration tests
   ↓
package/build
   ↓
deploy dev
   ↓
smoke/end-to-end validation
```

Do not build complex monorepo-aware deployment machinery before multiple projects create a genuine need.

Production-like deployment automation is valuable; speculative CI framework engineering is not.

[Back to top](#gods-freedom-ark)

---

# 23. Architecture Decision Records

Use an ADR for decisions that future-you may reasonably ask:

> Why did I choose this?

Location:

```text
docs/decisions/
```

Naming:

```text
ADR-001-use-parquet-for-curated-storage.md
ADR-002-partition-sales-by-sale-date.md
ADR-003-use-bigquery-merge.md
```

Recommended structure:

```text
Title
Status
Context
Options Considered
Decision
Rationale
Trade-offs
Consequences
```

Suggested ADR statuses:

```text
proposed
accepted
superseded
deprecated
```

Do not write ADRs for trivial coding choices.

[Back to top](#gods-freedom-ark)

---

# 24. Incident Documentation

Deliberately induced failures are part of the learning model.

Location:

```text
docs/incidents/
```

Naming:

```text
INC001-duplicate-batch.md
INC002-schema-drift.md
INC003-iam-denied.md
INC004-data-skew.md
```

Recommended structure:

```text
Title
Status
Impact
Symptoms
Detection
Investigation
Root Cause
Resolution
Validation
Prevention
Lessons Learned
```

Recommended statuses:

```text
open
mitigated
resolved
closed
```

An incident should train the workflow:

```text
symptom
  ↓
evidence
  ↓
root cause
  ↓
change
  ↓
rerun
  ↓
validate
  ↓
prevent recurrence
```

[Back to top](#gods-freedom-ark)

---

# 25. Dependency Management

Each engagement owns its runtime dependencies.

For Python projects, prefer a project-specific:

```text
pyproject.toml
```

Do not create one enormous root dependency file containing every tool used by every future project.

A future project may independently use:

```text
Python + PySpark
Scala + Flink
Python + Beam
dbt
Databricks
AWS Glue
```

and should not inherit unrelated dependencies.

Shared libraries may be introduced only when demonstrated reuse justifies them.

Pin or constrain dependencies enough to support reproducible environments.

[Back to top](#gods-freedom-ark)

---

# 26. Public Repository Hygiene

Assume anything committed to this repository may remain publicly available forever.

Never commit:

- real client data
- credentials
- API tokens
- passwords
- service-account keys
- private certificates
- private business records
- proprietary future client material
- Terraform state containing secrets
- `.env`
- large raw datasets

Safe public contents include:

- fictional/synthetic datasets
- appropriately licensed public datasets
- small representative samples
- code
- tests
- architecture diagrams
- benchmark generators
- documentation
- `.env.example`

Before every commit involving configuration, ask:

> Would I be comfortable if this file were copied and permanently public?

If not, do not commit it.

[Back to top](#gods-freedom-ark)

---

# 27. Initial Walmart-Readiness Engagements

The first fictional client is a large omnichannel retailer.

## P001 — Retail Sales Incremental Batch Pipeline

Architecture:

```text
POS Files
    ↓
GCS Raw
    ↓
Dataproc / PySpark
    ↓
Data Quality
 ┌──┴──────────┐
 ↓             ↓
Quarantine  Accepted
               ↓
          Curated GCS
               ↓
        BigQuery Staging
               ↓
             MERGE
               ↓
     Dimensional Warehouse
```

Primary skills:

- Python
- PySpark
- Dataproc
- GCS
- BigQuery
- advanced SQL
- explicit schemas
- data quality
- incremental loads
- idempotency
- deduplication
- partitioning
- late corrections
- Spark optimization
- BigQuery MERGE
- reconciliation
- observability
- performance benchmarking

This is the flagship engagement.

---

## P002 — Supplier Integration Pipeline

Architecture:

```text
CSV ──┐
JSON ─┼──> GCS → Validation → Normalization → BigQuery
API ──┘
                ↑
             Airflow
```

Primary skills:

- Python ingestion
- heterogeneous sources
- Airflow / Cloud Composer
- scheduling
- task dependencies
- retries
- exponential backoff
- backfills
- IAM
- service accounts
- missing-file handling
- schema normalization
- operational recovery

---

## P003 — Inventory Snapshot Pipeline

Architecture:

```text
Store / Product Inventory Snapshots
                ↓
             PySpark
                ↓
        BigQuery Historical Store
             ↙           ↘
     Current State    Historical Analytics
```

Primary skills:

- PySpark
- advanced SQL
- BigQuery
- snapshot facts
- composite keys
- warehouse modeling
- latest-known state
- historical state
- partitioning
- window functions
- late-arriving snapshots
- reconciliation

Optional extension:

```text
BigQuery → Power BI
```

[Back to top](#gods-freedom-ark)

---

# 28. Technology Coverage

The union of the initial engagements is intended to cover the current must-know stack.

| Technology / Competency | Primary Coverage |
|---|---|
| Python | P001, P002 |
| SQL | P001, P002, P003 |
| PySpark | P001, P003 |
| GCS | P001, P002 |
| Dataproc | P001 |
| BigQuery | P001, P002, P003 |
| Airflow / Composer | P002 |
| IAM / Service Accounts | Cross-project |
| Cloud Logging | Cross-project |
| Cloud Monitoring | Cross-project |
| Git / GitHub | Repository-wide |
| CI/CD | Repository-wide |
| Data Modeling | P001, P003 |
| Data Quality | All |
| Incremental Processing | P001 |
| Warehousing | P001, P003 |
| Performance Engineering | P001, P003 |
| Production Troubleshooting | Incident exercises |
| BI / Dashboarding | Optional extension |

Technologies such as Pub/Sub, Dataflow, Kafka, Terraform, Docker, Databricks, Snowflake, dbt, Flink, and lakehouse technologies may be added when future engagements justify them.

[Back to top](#gods-freedom-ark)

---

# 29. Explicitly Deferred Architecture

Do **not** build these merely because a future company might need them:

```text
GUI
REST API
multi-tenant control plane
RBAC implementation
metadata database
microservices
Kubernetes platform
billing system
team-management system
complex shared-package framework
custom plugin framework
enterprise identity system
```

The repository should remain compatible with adding such capabilities later, but current effort belongs in actual data engineering.

Likewise, directories such as:

```text
platform/
packages/
core/
interfaces/
```

should be introduced when real requirements appear.

[Back to top](#gods-freedom-ark)

---

# 30. Future Evolution

The repository is intentionally a **modular monorepo**, not a monolith.

Future engagements may independently use:

- AWS
- Azure
- Databricks
- Kafka
- Flink
- Dataflow
- dbt
- CDC
- streaming
- lakehouse architectures
- analytics engineering
- ML pipelines
- forecasting
- BI platforms

A future real company may eventually split the repository into:

```text
gfa-platform
gfa-common
client-c001-p001
client-c001-p002
client-c002-p003
...
```

The current architecture should make that extraction possible because engagement-specific implementation remains self-contained.

Future GUI/API adoption should reuse the same underlying domain concepts rather than duplicating business rules in interfaces.

[Back to top](#gods-freedom-ark)

---

# 31. Consistency Checklist for Every New Engagement

Before starting a new engagement, verify all of the following.

### Identity

- [ ] Unique `P###` engagement ID assigned
- [ ] Correct `C###` client ID referenced
- [ ] Directory follows naming convention
- [ ] `engagement.yaml` created

### Purpose

- [ ] Business problem is clearly stated
- [ ] Pipeline/data-product grain is understood
- [ ] Inputs and outputs are identified
- [ ] Architecture is chosen for the problem rather than technology collection

### Data

- [ ] Shared sources have data contracts
- [ ] Canonical data remains client-owned
- [ ] Project-owned outputs have clear locations
- [ ] Large data stays outside Git
- [ ] Sample data or deterministic generators exist where useful
- [ ] Lifecycle/retention is considered

### Configuration

- [ ] Environment-specific configuration is externalized
- [ ] No hard-coded prod resource names in application logic
- [ ] `.env.example` is safe to commit
- [ ] No secrets are committed

### Engineering

- [ ] Project is independently runnable
- [ ] Project does not import another engagement's internal code
- [ ] Dependencies are project-specific
- [ ] Tests are organized by purpose
- [ ] Data-quality checks are explicit
- [ ] Logs/run metadata carry enough context to debug failures

### Operations

- [ ] Rerun/idempotency behavior is understood
- [ ] Failure handling is considered
- [ ] Monitoring/logging strategy exists
- [ ] Benchmarking is used when performance matters
- [ ] Significant incidents are documented

### Documentation

- [ ] README explains how to understand and run the project
- [ ] Important design trade-offs are documented
- [ ] Significant architecture decisions receive ADRs
- [ ] Runbook exists when operational steps become nontrivial

### Git / CI

- [ ] Work occurs on an appropriate branch
- [ ] Commits are focused and meaningful
- [ ] Automated checks run where practical
- [ ] Public-repo hygiene has been reviewed before merge

If these checks pass, the engagement is consistent with the repository's v1 architecture.

[Back to top](#gods-freedom-ark)

---

# 32. Copyright

Copyright © 2026 Danny Han. All rights reserved.

This repository is currently published for portfolio, educational, and review purposes. No open-source license is granted unless explicitly stated otherwise for a specific component.

[Back to top](#gods-freedom-ark)
