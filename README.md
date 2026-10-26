# Project Atlas — Retail Data Engineering Pipeline & Analytics Dashboard

> An end-to-end retail analytics data engineering project built with **AWS S3, Databricks, PySpark, Delta Lake, and SQL**. Project Atlas ingests Olist e-commerce data, organizes it into Bronze/Silver/Gold layers, builds analytics-ready datasets, validates data quality and reconciliations, and records pipeline execution metadata for operational visibility.

**Repository:** [Saksham29-2004/Project-Atlas](https://github.com/Saksham29-2004/Project-Atlas)

---

## Table of contents

- [Overview](#overview)
- [Project goals](#project-goals)
- [Architecture](#architecture)
- [Data source and datasets](#data-source-and-datasets)
- [Technology stack](#technology-stack)
- [Repository contents](#repository-contents)
- [Pipeline walkthrough](#pipeline-walkthrough)
- [Data model and tables](#data-model-and-tables)
- [Data quality and validation](#data-quality-and-validation)
- [Analytics and business questions](#analytics-and-business-questions)
- [Dashboard](#dashboard)
- [How to run the notebooks](#how-to-run-the-notebooks)
- [Operational metadata and incremental file discovery](#operational-metadata-and-incremental-file-discovery)
- [Important metric definitions and caveats](#important-metric-definitions-and-caveats)
- [Results captured in notebook outputs](#results-captured-in-notebook-outputs)
- [Limitations and next steps](#limitations-and-next-steps)
- [Author](#author)

---

## Overview

Project Atlas is a cloud-based data engineering workflow for turning raw retail order data into structured, validated datasets that can support reporting and dashboarding.

The project is designed around a layered lakehouse pattern:

1. **Bronze** stores source data with ingestion metadata.
2. **Silver** standardizes and enriches source records and makes data-quality issues visible.
3. **Gold** produces business-facing aggregates for sales, category performance, and delivery performance.
4. **Operations metadata** records pipeline stages and file-processing status so that runs can be inspected and monitored.

Two Databricks notebooks are included in the published work:

- **`Project Atlas.ipynb`** — main pipeline notebook: environment checks, schema setup, ingestion, transformations, aggregations, audit logging, and data-quality/reconciliation checks.
- **`atlas_file_discovery.ipynb`** — source-file discovery notebook: compares CSV files currently present in S3 with a Delta manifest and classifies files that may need processing.

> **Scope note:** This README documents the implementation visible in the published notebooks and their recorded outputs. A configured Databricks job, schedule, alerting policy, or dashboard sharing permission should only be described as deployed if it is separately configured in the workspace.

## Project goals

- Ingest raw CSV datasets from Amazon S3 into Databricks.
- Organize data using a Bronze → Silver → Gold lakehouse structure.
- Use PySpark DataFrame transformations and Spark SQL for processing and analysis.
- Preserve lineage with source-file and ingestion metadata.
- Apply explicit data-quality checks instead of trusting row counts alone.
- Reconcile important Gold metrics against Silver source tables.
- Track pipeline stage status and processed-file metadata using Delta tables.
- Prepare curated datasets for a Databricks SQL dashboard.
- Demonstrate an end-to-end workflow that can be explained, tested, and extended.

## Architecture

```text
Olist raw CSV files
        |
        v
Amazon S3 (raw source folders)
        |
        +-------------------------------+
        |                               |
        v                               v
Main Databricks notebook        File-discovery notebook
        |                               |
        v                               v
Bronze Delta tables             Compare S3 files to Delta manifest
        |                               |
        v                               v
Silver cleaning + enrichment    NEW / CHANGED / METADATA_MISSING
        |                               |
        v                               v
Gold business aggregates        Databricks task values for orchestration
        |
        +--------------------------+
        |             |            |
        v             v            v
   Daily sales   Category       Delivery
                 performance    performance
        |
        v
Data validation + metric reconciliation
        |
        v
Databricks SQL dashboard / reporting
```

### Main platform components

- **Amazon S3:** stores the raw CSV source files.
- **Databricks:** provides the Spark execution environment, notebooks, catalog, and SQL analytics layer.
- **Apache Spark / PySpark:** performs distributed reads, transformations, joins, aggregations, and validation.
- **Delta Lake:** stores tables and supports table operations such as `MERGE`.
- **Unity Catalog-style three-part names:** tables are referenced with names such as `workspace.atlas_silver.orders`.
- **Databricks Jobs task values:** the discovery notebook publishes values such as `has_pending_files` and `pending_file_paths` for downstream workflow logic.

## Data source and datasets

The notebook paths identify the project data as **Olist e-commerce retail data** stored in an S3 bucket. The source folders used by the notebooks are:

- `raw/olist/orders/`
- `raw/olist/order_items/`
- `raw/olist/products/`

The main notebook reads CSV data from S3 with a header row and inferred schema, then writes the datasets into managed Delta tables.

### Source datasets used in the core pipeline

| Dataset | Grain / role | Expected row count recorded in notebook |
|---|---|---:|
| `orders` | One record per order | 99,441 |
| `order_items` | One record per order line item | 112,650 |
| `products` | One record per product | 32,951 |

These are the expected counts encoded in the notebook's checks. They are useful validation targets for this dataset snapshot, not universal guarantees for every source version.

### What the source data enables

The available fields support analysis of:

- order purchase dates and order statuses;
- customers associated with orders;
- item prices and freight values;
- product identifiers and product categories;
- delivered-order timing versus estimated delivery timing.

The notebooks also inspect order-status distributions, timestamp completeness, null dates, and joins between orders, items, and products.

## Technology stack

| Technology | Role in Project Atlas |
|---|---|
| Amazon S3 | Raw CSV storage |
| Databricks notebooks | Development and execution of pipeline stages |
| Apache Spark | Distributed data processing engine |
| PySpark | DataFrame transformations, joins, aggregations, and quality checks |
| Spark SQL | Table creation, SQL exploration, validation queries, and operational views |
| Delta Lake | Persistent tables, merge/upsert operations, and table format |
| Databricks catalog and schemas | Namespaces for Bronze, Silver, Gold, and operational tables |
| Databricks Jobs task values | Passing discovery results to workflow tasks |
| Databricks SQL dashboards | Intended presentation layer for curated analytics |

## Repository contents

The repository currently contains the notebooks used to document and run the work. If you organize them into a `notebooks/` directory, a suggested structure is:

```text
Project-Atlas/
├── README.md
├── notebooks/
│   ├── Project Atlas.ipynb
│   └── atlas_file_discovery.ipynb
└── docs/
    └── images/
        └── project-atlas-dashboard1.png
        └── project-atlas-dashboard2.png
        └── project-atlas-dashboard3.png
        └── project-atlas-dashboard4.png
```

If the notebooks are stored in the repository root instead, update the paths above to match the actual repository structure. The dashboard image path is a recommended location; add the real screenshot file there before expecting it to render in GitHub.

## Pipeline walkthrough

### 1. Environment and catalog checks

The main notebook begins by checking the Spark version and current catalog/schema context, listing catalogs and schemas, and testing that the environment can write and read a small Delta table. This helps distinguish a workspace/catalog configuration problem from a pipeline problem.

The notebook creates the project schemas when needed:

- `workspace.atlas_bronze`
- `workspace.atlas_silver`
- `workspace.atlas_gold`
- `workspace.atlas_ops`

### 2. Bronze ingestion

The notebook reads the raw CSV files from S3 and persists source datasets in Bronze Delta tables.

Bronze tables:

- `workspace.atlas_bronze.orders`
- `workspace.atlas_bronze.order_items`
- `workspace.atlas_bronze.products`

The notebook also inspects ingestion metadata such as `_ingested_at`, `_source_file`, and batch-related fields. Keeping source metadata alongside records helps trace data back to its origin and understand which run processed it.

The notebook checks row counts against expected values:

| Table | Expected rows |
|---|---:|
| Bronze orders | 99,441 |
| Bronze order items | 112,650 |
| Bronze products | 32,951 |

### 3. Silver cleaning and standardization

Silver tables are derived from Bronze tables. The visible transformations include trimming identifiers, converting timestamp fields, using explicit numeric types where appropriate, and creating `_dq_...` flags to make potential data-quality issues observable.

Silver tables:

- `workspace.atlas_silver.orders`
- `workspace.atlas_silver.order_items`
- `workspace.atlas_silver.products`

The notebook checks missing identifiers, invalid timestamps, invalid prices/freight values, duplicate keys, and referential integrity. The goal is to retain visibility into quality issues rather than silently hiding them.

### 4. Enrichment

The notebook joins Silver order items with order and product information to create:

- `workspace.atlas_silver.order_item_enriched`

This enriched table is used for category-level analysis and supports combining item-level sales with product attributes and order context.

A specific validation checks that enrichment preserves the number of order-item rows. This is important because an unintended one-to-many join can inflate sales totals.

### 5. Gold business aggregates

Gold tables provide datasets intended for analytics and dashboarding.

#### `workspace.atlas_gold.daily_sales`

Daily sales and order-level metrics grouped by purchase date. The schema shown in the notebook includes:

- `purchase_date`
- `order_count`
- `customer_count`
- `delivered_orders`
- `cancelled_orders`
- `items_sold`
- `items_sales_value`
- `freight_value`

#### `workspace.atlas_gold.category_performance`

Category-level aggregates derived from the enriched order-item dataset. The notebook examines item counts, item sales value, and freight value by category.

#### `workspace.atlas_gold.delivery_performance`

Daily delivery-performance aggregates for eligible delivered orders. The notebook checks actual versus estimated delivery timing, late-order counts, and average delivery durations.

### 6. Audit logging

The notebook creates an operational audit table:

`workspace.atlas_ops.pipeline_run_audit`

The recorded fields include:

- run and batch identifiers;
- pipeline name and stage;
- status;
- start and completion timestamps;
- source file count;
- records processed;
- error message;
- audit creation timestamp.

The pipeline code wraps major processing stages in error handling and records stage outcomes. This gives the operator a place to investigate which stage ran, how many records it processed, and whether an error was recorded.

### 7. File manifest

The notebook uses a Delta manifest table:

`workspace.atlas_bronze.file_manifest`

It records source paths and file metadata, including processing status, file size, last-modified information, and processing time. The main notebook updates the manifest with a Delta `MERGE`, matching on `source_path`, updating metadata for existing paths, and inserting previously unseen paths.

### 8. File discovery and change detection

`atlas_file_discovery.ipynb` lists CSV files in the configured S3 source folders, compares their current metadata with the manifest, and classifies them into statuses including:

- **`NEW`** — the source path is not present in the manifest;
- **`METADATA_MISSING`** — a manifest record exists but required comparison metadata is missing;
- **`CHANGED`** — the current file size or modification timestamp differs from the manifest.

The notebook filters these statuses to identify files that need processing and publishes task values:

- `has_pending_files`
- `pending_file_paths`

These values are intended to support conditional downstream task execution in a Databricks workflow. File discovery itself does not prove that a complete production orchestration schedule or retry policy has been configured.

## Data model and tables

The following table inventory reflects the names referenced in the notebooks.

| Layer / schema | Table | Purpose |
|---|---|---|
| Bronze | `workspace.atlas_bronze.orders` | Raw orders plus ingestion metadata |
| Bronze | `workspace.atlas_bronze.order_items` | Raw order-line items plus ingestion metadata |
| Bronze | `workspace.atlas_bronze.products` | Raw product records plus ingestion metadata |
| Bronze / metadata | `workspace.atlas_bronze.file_manifest` | Source-file processing and metadata manifest |
| Silver | `workspace.atlas_silver.orders` | Standardized order records and quality flags |
| Silver | `workspace.atlas_silver.order_items` | Standardized item records and quality flags |
| Silver | `workspace.atlas_silver.products` | Standardized product records and quality flags |
| Silver | `workspace.atlas_silver.order_item_enriched` | Enriched item-level data for analytics |
| Gold | `workspace.atlas_gold.daily_sales` | Daily sales, order, customer, item, and freight metrics |
| Gold | `workspace.atlas_gold.category_performance` | Category-level sales and item metrics |
| Gold | `workspace.atlas_gold.delivery_performance` | Daily delivery-performance metrics |
| Operations | `workspace.atlas_ops.pipeline_run_audit` | Historical stage execution audit records |
| Operations | `workspace.atlas_ops.latest_stage_status` | Latest recorded status per pipeline stage |
| Operations | `workspace.atlas_ops.pipeline_health` | Summary of latest stage statuses |

### Why the layers matter

- **Bronze** preserves a close representation of the ingested source and supports traceability.
- **Silver** provides cleaned, typed, joinable data with explicit quality signals.
- **Gold** reduces repeated transformation work for reporting and presents metrics at business-friendly grains.
- **Operations tables** make execution and source-file state queryable instead of leaving all run history in notebook output.

## Data quality and validation

The notebooks include several classes of checks.

### Structural and key checks

- Expected row counts for the core source datasets.
- Duplicate order-ID detection.
- Duplicate `(order_id, order_item_id)` detection.
- Non-null order identifiers.
- Non-null purchase timestamps.
- Enrichment row-count preservation.

### Referential integrity checks

- Order-item order IDs are checked against the Silver orders table.
- Non-null item product IDs are checked against the Silver products table.
- The notebook uses anti-joins to identify unmatched keys.

### Business-rule checks

- Item prices must not be negative.
- Freight values must not be negative.
- Required order IDs and purchase timestamps must be present.

### Cross-layer reconciliation

The notebook compares totals in Silver against corresponding Gold aggregates, including:

- item count;
- item sales value;
- order count;
- customer count;
- cancelled orders;
- delivered orders.

The checks print `PASS`/`FAIL` and raise an exception when a required check fails. This is stronger than merely displaying totals because a failed reconciliation can stop the notebook rather than pass unnoticed.

## Analytics and business questions

The Gold datasets support questions such as:

### Sales and demand

- How do item sales and order volumes change over time?
- Which dates have the highest or lowest item sales value?
- How many items were sold and how much item sales value was recorded?
- How does freight value change alongside item sales?

### Product category performance

- Which categories contribute the most item sales value?
- Which categories account for the highest item counts?
- How does freight value vary across categories?

### Delivery performance

- How many eligible orders were delivered?
- How many orders were late relative to the estimated delivery date?
- What are the average actual and estimated delivery durations?
- How does delivery performance vary by purchase date?

### Operational monitoring

- Which pipeline stages most recently succeeded or failed?
- When was the latest audit record created?
- How many source files are pending processing?
- Has a file's size or last-modified timestamp changed since it was last processed?

These are supported analysis directions based on the tables and SQL/PySpark queries present in the notebooks; the exact charts and filters shown in the dashboard depend on the dashboard configuration.

## Dashboard

The dashboard is the presentation layer for the curated Gold data. It should make key business metrics understandable without requiring the viewer to inspect notebook code.

**Recommended screenshot location:** `docs/images/project-atlas-dashboard.png`

Once you have saved the actual Databricks dashboard screenshot at that path and committed it to GitHub, the image will render below:

![Project Atlas Databricks dashboard](docs/images/project-atlas-dashboard.png)

> **Screenshot setup:** create `docs/images/` in the repository, save a screenshot of the final dashboard as `project-atlas-dashboard.png`, and commit it. The notebooks uploaded for this documentation do not themselves provide a standalone dashboard screenshot, so this image path is a placeholder until the real screenshot is added.

### Suggested dashboard sections

Use only visuals that correspond to fields available in the Gold tables.

1. **Executive KPI tiles**
   - Item sales value
   - Items sold
   - Orders
   - Cancelled orders
   - Delivered orders
2. **Sales trend**
   - Item sales value over purchase date or month
   - Order count over time
3. **Category performance**
   - Item sales value by product category
   - Items sold by category
4. **Delivery performance**
   - Late orders over time
   - Average actual versus estimated delivery days
5. **Pipeline health** (if operational visibility is included)
   - Latest stage status
   - Failed versus successful stages
   - Most recent audit timestamp

### Metric interpretation

The dashboard should label **item sales value** distinctly from total order value or recognized revenue. The current notebook sums item `price`; freight is tracked separately. Do not label item sales value as profit, net revenue, or total customer spend unless those definitions are explicitly implemented.

## How to run the notebooks

These notebooks are written for a **Databricks environment** and are not expected to run unchanged in a local Python interpreter.

### Prerequisites

- A Databricks workspace with a compatible Spark environment.
- Access to the configured S3 bucket and source folders.
- Permissions to read source objects and write Delta tables in the configured catalog/schema.
- A catalog/environment where the `workspace` catalog and required schemas are available, or corresponding updates to the table names.
- Databricks utilities (`dbutils`) for file listing and job task values.
- A workflow/job configuration if the task values are to be consumed by downstream tasks.

### Execution outline

1. Upload/import `Project Atlas.ipynb` into Databricks.
2. Confirm the source S3 paths and credentials/access configuration.
3. Run the environment checks and schema/table setup.
4. Execute the notebook cells in order so that audit, Bronze, Silver, enrichment, and Gold dependencies exist before downstream steps.
5. Review the notebook output and inspect `workspace.atlas_ops.pipeline_run_audit`.
6. Run the data-quality and reconciliation cells; resolve any failures before relying on the Gold tables.
7. Import and configure `atlas_file_discovery.ipynb` as a workflow task if you want to use its task values for conditional processing.
8. Build or refresh a Databricks SQL dashboard from the Gold tables.
9. Add a screenshot of the dashboard to `docs/images/project-atlas-dashboard.png`.

### Environment-specific configuration

The notebook contains a specific S3 bucket path and three-part table names. Before running in another workspace, replace these with the correct bucket, catalog, schema, and access configuration. Avoid committing credentials, access keys, tokens, or other secrets to GitHub.

## Operational metadata and incremental file discovery

The discovery notebook compares a live S3 listing with the manifest table. It uses file path, size, and modification metadata to detect files that are new or appear changed.

This design helps avoid blindly treating every file as new on every run. It also provides an interface for a workflow to decide whether downstream processing is needed.

**Important boundary:** metadata comparison is not a content hash. A file can change without a reliable size/timestamp difference in some storage or copy workflows, and identical metadata does not mathematically prove identical contents. For stronger change detection, consider storing an object version ID or checksum where available.

The manifest update uses Delta `MERGE` to update matching paths and insert new paths. If multiple workflow runs can process the same files concurrently, add explicit concurrency/idempotency design and test it in the target job configuration.

## Important metric definitions and caveats

- **Item sales value:** sum of item `price` values. It excludes freight in the Gold field used by the notebook.
- **Freight value:** tracked separately from item sales value.
- **Order count:** aggregated order counts should be interpreted at the Gold table's grain and reconciled against distinct order IDs at source level.
- **Customer count:** the notebook reconciles this against distinct customer IDs in Silver. A customer may place multiple orders, so a daily customer count summed across days is not necessarily the same as the number of unique customers over the entire period.
- **Delivered orders:** some delivery analyses filter to delivered orders with non-null purchase, actual-delivery, and estimated-delivery timestamps. That eligible subset can differ from the overall count of records whose status is `delivered`.
- **Cancelled orders:** date-based aggregates can omit cancellations that do not have a usable purchase date. The notebook investigates null timestamps and cancellation reconciliation explicitly.
- **Average delivery days:** an average of daily averages is not generally equal to the overall order-weighted average. Use weighted calculations if a dashboard requires an overall average.
- **Row counts:** expected counts are tied to the source snapshot and can change if the input dataset changes.

## Results captured in notebook outputs

The notebook outputs include successful checks for the following values and conditions:

| Validation | Recorded result |
|---|---:|
| Silver order rows / distinct orders | 99,441 |
| Silver order-item rows | 112,650 |
| Silver product rows | 32,951 expected by the notebook |
| Enriched order-item rows | 112,650 |
| Item count reconciled from Silver to Gold | 112,650 = 112,650 |
| Item sales value reconciled from Silver to Gold | 13,591,643.70 = 13,591,643.70 |
| Order count reconciled from Silver to Gold | 99,441 = 99,441 |
| Distinct customer count in source reconciliation | 99,441 = 99,441 |
| Cancelled orders in reconciliation | 625 = 625 |
| Delivered orders in reconciliation | 96,478 = 96,478 |
| Duplicate order-ID groups | 0 |
| Duplicate order-item key groups | 0 |
| Unmatched order IDs for items | 0 |
| Unmatched product IDs for items | 0 |
| Negative item-price rows | 0 |
| Negative freight rows | 0 |
| Null order IDs | 0 |
| Null purchase timestamps | 0 |

The notebook also contains a delivery eligibility check with **96,470** eligible delivered orders under stricter timestamp completeness criteria. This differs from the **96,478** overall delivered-order count used in the separate reconciliation, because the eligibility filters are not identical. Keep these definitions distinct in reporting.
