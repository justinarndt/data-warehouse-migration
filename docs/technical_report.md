# Technical Report: JD Edwards Data Warehouse Modernization on Azure

**Author:** Justin Arndt
**Date:** February 2026
**Platform:** Microsoft Azure (ADF, ADLS Gen2, Azure SQL Database)
**Budget Constraint:** < $200 Azure Credits

---

## 1. Executive Summary

This project implements an end-to-end data warehouse migration that takes legacy JD Edwards (JDE) EnterpriseOne ERP data — with its archaic Julian dates, implicit decimal encoding, and cryptic column naming — and transforms it into a modern, cloud-native analytics platform on Azure. The architecture follows the **Medallion pattern** (Bronze → Silver → Gold), orchestrated entirely through Azure Data Factory with metadata-driven pipelines, Slowly Changing Dimension Type 2 tracking, and automated failure alerting via Logic Apps.

The resulting platform delivers a Kimball-style star schema optimized for Power BI consumption, with full historical auditability and point-in-time analytical accuracy — all running on under $10/month of Azure resources.

---

## 2. Source System Analysis

### 2.1 JD Edwards EnterpriseOne

JD Edwards originated in the 1970s on IBM AS/400 systems where storage was measured in kilobytes. This heritage produced data encoding conventions that persist today and silently corrupt modern analytics tools when not properly handled.

### 2.2 Julian Date Format (CYYDDD)

JDE stores all dates as 6-digit integers in `CYYDDD` format — a space-saving encoding from an era when each saved byte mattered:

| Component | Meaning | Value Range |
|-----------|---------|-------------|
| **C** | Century indicator | `0` = 1900s, `1` = 2000s, `2` = 2100s |
| **YY** | Two-digit year | `00`–`99` |
| **DDD** | Day of year (ordinal) | `001`–`366` |

**Conversion formula:**
```
Gregorian Year = 1900 + (C × 100) + YY
Gregorian Date = January 1 of that year + (DDD − 1) days
```

**Example:** `123001` → Century 1 (2000s) + Year 23 + Day 001 = **January 1, 2023**

If ingested as a raw integer without conversion, reporting tools interpret this as "year 123" or fail to parse entirely — rendering all time-series analysis meaningless.

### 2.3 Implicit Decimal Precision

JDE stores financial values as integers with decimal places defined externally in the Data Dictionary. For columns with 2 display decimals:

| Raw JDE Value | Actual Value | Scaling |
|---------------|-------------|---------|
| `1050` | $10.50 | ÷ 100 |
| `250000` | $2,500.00 | ÷ 100 |
| `1430130` | $14,301.30 | ÷ 100 |

**The risk:** If this transformation is missed, every revenue figure in the Gold layer is overstated by a factor of 100. A $10.50 line item appears as $1,050 — a catastrophic error in financial reporting that may not be immediately obvious in aggregate dashboards.

### 2.4 Source Tables

| Table | Description | Key Columns | Role |
|-------|-------------|-------------|------|
| **F0101** | Address Book Master | `ABAN8` (Customer ID), `ABALPH` (Name), `ABAT1` (Type), `ABAC01` (Category), `ABUPMJ` (Date Updated — Julian) | Customer Dimension |
| **F4211** | Sales Order Detail | `SDDOCO` (Order #), `SDDCTO` (Type), `SDAN8` (Customer FK), `SDLITM` (Item), `SDTRDJ` (Order Date — Julian), `SDUORG` (Units — implicit decimal), `SDAEXP` (Extended Price — implicit decimal) | Sales Fact |

---

## 3. Architecture Design

### 3.1 Medallion Architecture

The Medallion Architecture provides a layered approach that separates raw ingestion, business logic transformation, and analytical consumption into distinct, independently testable zones.

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   SOURCE    │     │   BRONZE     │     │   SILVER     │     │    GOLD      │
│             │     │              │     │              │     │              │
│  JDE ERP    │────▶│  Immutable   │────▶│  Cleansed    │────▶│  Star Schema │
│  F0101      │     │  Raw Parquet │     │  Decoded     │     │  Dim + Fact  │
│  F4211      │     │  Partitioned │     │  Historized  │     │  Indexed     │
│             │     │  ADLS Gen2   │     │  ADF Flows   │     │  Azure SQL   │
└─────────────┘     └──────────────┘     └──────────────┘     └──────┬───────┘
                                                                      │
                                                               ┌──────▼───────┐
                                                               │  Power BI /  │
                                                               │  Tableau     │
                                                               └──────────────┘
```

**Why Medallion over direct ETL?**
- **Replayability:** If Silver transformation logic has a bug, Bronze data is untouched — fix the logic and re-run without re-extracting from the production ERP
- **Auditability:** Raw data is preserved exactly as received, satisfying SOX/compliance requirements
- **Decoupling:** Each layer can be developed, tested, and scaled independently

### 3.2 Technology Stack

| Component | Technology | Justification |
|-----------|-----------|---------------|
| **Storage** | ADLS Gen2 (HNS enabled) | Directory manipulation + atomic operations required for Big Data workloads; HNS converts flat Blob storage into a true filesystem |
| **Orchestration** | Azure Data Factory | Managed ETL service with visual data flows on ephemeral Spark clusters — no infrastructure to manage |
| **Transformation** | ADF Mapping Data Flows | Spark-based execution engine with visual debugging; more scalable than stored procedures for large datasets |
| **Serving** | Azure SQL Database (Basic, 5 DTU) | Sufficient for portfolio demo volumes; forces efficient SQL design (batched inserts, careful indexing) |
| **Security** | Key Vault + Managed Identity | Zero credentials in code; secrets retrieved dynamically at pipeline runtime |
| **Alerting** | Azure Logic Apps | Rich HTML email notifications on pipeline failure with error context |
| **CI/CD** | GitHub Actions | Automated validation of Python, SQL syntax, and ADF JSON on every push |

### 3.3 Storage Layout

```
datalake/
├── 01-bronze/
│   ├── landing/           # Raw CSV drop zone (upload point)
│   │   ├── F0101.csv
│   │   └── F4211.csv
│   └── jde/               # Converted Parquet (time-partitioned)
│       ├── F0101/
│       │   └── 2026/02/12/
│       └── F4211/
│           └── 2026/02/12/
├── 02-silver/
│   └── sales/
│       └── transactions/  # Decoded, normalized Parquet
├── 03-gold/               # Reserved for any Parquet exports
└── config/
    └── source_config.json # Metadata-driven ingestion config
```

The time-based partitioning (`Year/Month/Day`) under Bronze enables **partition pruning** — downstream Spark jobs only scan relevant date folders rather than the entire lake, reducing compute costs proportionally.

---

## 4. Pipeline Architecture

### 4.1 Pipeline Hierarchy

```
PL_Master (Orchestrator)
├── Execute_Bronze ──▶ PL_Ingest_Bronze
│   ├── Lookup: source_config.json
│   └── ForEach: CSV → Parquet (partitioned)
├── Execute_Silver ──▶ PL_Transform_Silver
│   ├── DF_Clean_JDE (Julian dates + decimals)
│   └── DF_SCD2_Customer (hash-based change detection)
├── Execute_Gold ──▶ PL_Load_Gold
│   └── DF_Load_FactSales (point-in-time joins)
└── OnFailure_*_Alert ──▶ Logic App (POST with error context)
```

### 4.2 Bronze Layer — Metadata-Driven Ingestion

The ingestion pipeline avoids hardcoded file paths — an anti-pattern in enterprise environments where source files change frequently.

**Design:**
1. A **Lookup** activity reads `source_config.json` from ADLS, which contains an array of `{sourceFile, sinkPath}` objects
2. A **ForEach** activity iterates over each entry
3. Inside the loop, a parameterized **Copy** activity reads CSV from the landing zone and writes Parquet to the partitioned Bronze area

**Parquet conversion rationale:**

| Property | CSV | Parquet |
|----------|-----|---------|
| Storage format | Row-based text | Columnar binary |
| Compression | ~1:1 | ~10:1 (Snappy) |
| Schema enforcement | None (inferred) | Embedded |
| Column pruning | Full scan required | Read only needed columns |

Converting at the Bronze boundary means every downstream consumer benefits from columnar efficiency without re-processing.

### 4.3 Silver Layer — JDE Decoder Data Flow

`DF_Clean_JDE` is a Mapping Data Flow that executes on an ephemeral Spark cluster and performs two critical transformations:

**Julian Date Conversion Expression (ADF expression language):**
```
toDate(
    toString(
        1900 + toInteger(substring(toString(SDTRDJ), 1, 1)) * 100 +
        toInteger(substring(toString(SDTRDJ), 2, 2))
    ) + '-01-01'
) + days(toInteger(substring(toString(SDTRDJ), 4, 3)) - 1)
```

This expression:
1. Extracts the century indicator (position 1) and multiplies by 100
2. Adds the two-digit year (positions 2–3) to the base year 1900
3. Constructs a January 1st date for that year
4. Adds `DDD − 1` days to reach the correct ordinal date

**Implicit Decimal Conversion:**
```
toFloat(SDAEXP) / 100.00
```

The cast to `toFloat()` before division prevents integer truncation. Using `100.00` (float literal) ensures the result maintains decimal precision.

**Column Renaming:** Cryptic JDE abbreviations are mapped to business-friendly names:

| JDE Column | Business Name | Transformation |
|-----------|--------------|----------------|
| `SDDOCO` | `OrderNumber` | Direct mapping |
| `SDAN8` | `CustomerID` | Direct mapping |
| `SDTRDJ` | `OrderDate` | Julian → Date |
| `SDAEXP` | `ExtendedAmount` | ÷ 100 |
| `SDUORG` | `Quantity` | ÷ 100 |
| — | `UnitPrice` | Derived: Amount ÷ Quantity |
| — | `DateKey` | Derived: YYYYMMDD integer |

### 4.4 Silver Layer — SCD Type 2 Customer Dimension

`DF_SCD2_Customer` implements Slowly Changing Dimensions Type 2 to preserve the full history of customer attribute changes. This is essential for accurate historical reporting — if a customer moved regions in 2024, their 2023 sales should still report against the 2023 region.

**The Hash Comparison Pattern:**

Rather than comparing every business column individually (which scales poorly), we compute a SHA256 hash of all tracked columns and compare a single value:

```
sha2(256, ABALPH, ABAT1, ABAC01)
```

**Flow logic:**

```
┌──────────────┐   ┌──────────────┐
│  New F0101   │   │  Existing    │
│  from Bronze │   │  Dim_Customer│
└──────┬───────┘   └──────┬───────┘
       │                   │
   Hash columns        Hash columns
       │                   │
       └────────┬──────────┘
                │
          Lookup on ABAN8
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
   NEW ROW   CHANGED   NO CHANGE
   (no match) (hash ≠)  (hash =)
       │        │        │
       │    ┌───┴───┐    └─ Discard
       │    │       │
       │  UPDATE  INSERT
       │  old row  new version
       │  Active=0 Active=1
       │  ValidTo= ValidFrom=
       │   now()    now()
       │            │
       └────────────┘
              │
         INSERT new
         Active=1
         ValidTo=NULL
```

**Critical detail — Sink Ordering:** The Update sink (which expires old rows) must execute *before* the Insert sink (which creates new versions). Without this ordering, it's possible to insert a new active row and then immediately expire it, corrupting the dimension. ADF controls this via the `sinkOrder` property (Update = 1, Insert = 2).

---

## 5. Gold Layer — Star Schema Design

### 5.1 Dimensional Model

The Gold layer implements a Kimball-style star schema optimized for OLAP query patterns:

```
                    ┌──────────────────┐
                    │   Dim_Date       │
                    │──────────────────│
                    │ DateKey (PK)     │
                    │ FullDate         │
                    │ DayOfWeekName    │
                    │ MonthName        │
                    │ Quarter          │
                    │ Year             │
                    │ IsWeekend        │
                    │ FiscalQuarter    │
                    └────────┬─────────┘
                             │
┌──────────────────┐   ┌─────┴────────────┐
│  Dim_Customer    │   │   Fact_Sales     │
│──────────────────│   │──────────────────│
│ CustomerKey (PK) │◄──│ CustomerKey (FK) │
│ CustomerID       │   │ DateKey (FK)     │
│ CustomerName     │   │ OrderNumber (DD) │
│ SearchType       │   │ OrderType (DD)   │
│ CategoryCode     │   │ ItemNumber (DD)  │
│ IsActive         │   │ Quantity         │
│ ValidFrom        │   │ ExtendedAmount   │
│ ValidTo          │   │ UnitPrice        │
│ RowHash          │   │ LoadDate         │
└──────────────────┘   └──────────────────┘
                       (DD = Degenerate Dimension)
```

### 5.2 Dim_Date

A pre-populated calendar dimension spanning **2020–2040** (~7,670 rows). This table is generated once by a T-SQL script and provides the calendar backbone required for Power BI time-intelligence functions (YoY growth, rolling averages, same-period-last-year comparisons).

Key columns include `IsWeekend`, `FiscalQuarter`, and `IsLeapYear` — all pre-computed to avoid runtime calculation overhead in reports.

### 5.3 Dim_Customer (SCD Type 2)

Each row represents a **version** of a customer. When a customer's name, type, or category changes, the current row is expired (`IsActive = 0`, `ValidTo = now()`) and a new row is inserted (`IsActive = 1`, `ValidTo = NULL`).

**Indexes** are tuned for the two primary access patterns:
- **SCD2 merge lookups:** Clustered on `(CustomerID, IsActive)` for fast active-row retrieval
- **Point-in-time joins:** Covering index on `(CustomerID, ValidFrom, ValidTo)` including `CustomerKey`

### 5.4 Fact_Sales

The fact table contains **measures** (Quantity, ExtendedAmount, UnitPrice) and **foreign keys** to dimensions. `OrderNumber`, `OrderType`, and `ItemNumber` are retained as **degenerate dimensions** — attributes stored directly on the fact table because they don't warrant separate dimension tables.

### 5.5 The Point-in-Time Join

This is the most analytically important join in the warehouse. A naive join (`Sales.CustomerID = Customer.CustomerID`) would always attribute sales to the *current* customer state. The correct join is:

```sql
F4211.ABAN8 = Dim_Customer.CustomerID
AND F4211.OrderDate >= Dim_Customer.ValidFrom
AND F4211.OrderDate < ISNULL(Dim_Customer.ValidTo, '9999-12-31')
```

This ensures a sale made in January 2024 is attributed to the customer's January 2024 attributes — not their current (potentially different) attributes.

---

## 6. Security Architecture

```
┌─────────────┐      ┌───────────────┐      ┌──────────────┐
│    ADF      │─────▶│  Key Vault    │─────▶│  ADLS Gen2   │
│  Managed    │ Get/ │  Secrets:     │      │  (via key)   │
│  Identity   │ List │  - adls-key   │      └──────────────┘
│             │      │  - sql-conn   │      ┌──────────────┐
│             │      │               │─────▶│  Azure SQL   │
└─────────────┘      └───────────────┘      │  (via conn)  │
                                            └──────────────┘
```

**Principles applied:**
- **Zero credentials in code:** All secrets stored in Key Vault, referenced dynamically at pipeline runtime
- **Managed Identity authentication:** ADF uses its system-assigned identity — no service principal passwords to rotate
- **Least privilege:** ADF identity has only `Get` and `List` on Key Vault secrets; `Storage Blob Data Contributor` on ADLS — no admin access
- **Public portfolio safe:** The Git repository contains only resource *names* and *structure*, never actual keys or connection strings

---

## 7. Error Handling & Monitoring

The Master Pipeline implements a **Try-Catch** pattern at each stage:

```
Execute_Bronze ──(success)──▶ Execute_Silver ──(success)──▶ Execute_Gold
      │                             │                             │
   (failure)                     (failure)                     (failure)
      │                             │                             │
      ▼                             ▼                             ▼
 WebActivity POST            WebActivity POST            WebActivity POST
 to Logic App                to Logic App                to Logic App
```

Each failure handler sends a JSON payload to an Azure Logic App HTTP trigger:
```json
{
    "message": "<error from failed activity>",
    "pipeline": "PL_Master",
    "runId": "<ADF run GUID>",
    "stage": "Bronze | Silver | Gold",
    "timestamp": "<UTC timestamp>"
}
```

The Logic App processes this payload and sends a rich HTML email via the Office 365 connector, providing the engineering team with immediate, actionable context about which stage failed and why.

---

## 8. Cost Engineering

A primary constraint of this project is the **$200 Azure credit limit**. Every architectural decision was evaluated against cost:

| Decision | Enterprise Default | This Project | Monthly Savings |
|---------|-------------------|-------------|-----------------|
| SQL tier | Synapse Dedicated Pool (~$1,200/mo) | SQL Basic (5 DTU) | ~$1,195 |
| Spark clusters | Always-on Databricks | ADF Data Flows (TTL=0) | ~$400+ |
| Storage tier | Hot | Cool + lifecycle policies | ~60% storage cost |
| Networking | Private Link + VNet | Public endpoints + firewall rules | ~$50 |

**Data Flow TTL = 0** is the most impactful setting. By default, ADF keeps Spark clusters warm after a Data Flow execution for faster subsequent runs. Setting TTL to 0 shuts down clusters immediately, eliminating idle compute charges entirely. The trade-off is a ~5-minute cold-start on the next run — acceptable for a portfolio demonstration.

**Actual monthly cost:** Under **$10/month** for this workload volume.

---

## 9. CI/CD Pipeline

The GitHub Actions workflow validates three categories on every push to `main`:

| Job | Checks | Failure Condition |
|-----|--------|-------------------|
| **validate-python** | flake8 lint + smoke test (generate CSVs) | Syntax errors, undefined names, or missing output files |
| **validate-sql** | File existence + non-empty check | Any SQL script is missing or empty |
| **validate-adf-json** | JSON syntax validation + component inventory | Invalid JSON or missing required ADF components |

The ADF instance is connected to the GitHub repo with the collaboration branch set to `main` and publish branch set to `adf_publish`. This mirrors a real-world Dev/Test/Prod flow where changes are authored on feature branches, merged to `main` for integration testing, and published to `adf_publish` for production deployment.

---

## 10. Data Lineage Summary

```
F0101.csv ──▶ Bronze Parquet ──▶ DF_SCD2_Customer ──▶ Gold.Dim_Customer
  (50 rows)    (immutable)        (hash + merge)       (versioned rows)

F4211.csv ──▶ Bronze Parquet ──▶ DF_Clean_JDE ──▶ Silver Parquet ──▶ Gold.Fact_Sales
  (200 rows)   (immutable)       (decode JDE)     (normalized)       (star schema)
                                                                      ↕ joins
                                                                   Gold.Dim_Date
                                                                   Gold.Dim_Customer
```

Every transformation is traceable from source CSV through each Medallion layer to the final star schema, with Bronze preserving the raw state for replay at any time.

---

## 11. Conclusion

This project demonstrates a complete lifecycle implementation of a modern data platform — from source system analysis through dimensional modeling, ETL orchestration, security hardening, and operational monitoring. The specific challenges addressed (Julian date decoding, implicit decimal normalization, SCD Type 2 merges within a distributed Spark environment, and point-in-time dimensional joins) represent the kind of nuanced, high-difficulty data engineering work typically encountered in Fortune 500 JDE migration initiatives.

The strict adherence to the $200 budget constraint further demonstrates FinOps maturity — the ability to deliver enterprise-grade architecture patterns on consumption-based pricing, a highly sought-after skill in modern DataOps roles.
