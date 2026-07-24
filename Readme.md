# Healthcare Claims Payment Reconciliation

A serverless data engineering pipeline on Databricks that ingests CMS Medicare claims, validates them, and produces business-ready tables answering one question: **where is payment leaking, and why?**

Built to compare against production claims reconciliation work I did on Talend and SQL Server — same problem, lakehouse architecture.

---

## The findings

**Medicare overpaid on 22 procedure codes.** Payments exceeded the allowed amount by up to 126% of the agreed rate, concentrated in clinical chemistry panels (HCPCS 83xxx). Small in absolute dollars against a $386M total, but a systematic pattern rather than scattered noise — the kind of thing a payment integrity team would want flagged.

**24.4% of professional service lines were paid zero.** These are lines where a service was billed and processed but no payment issued — the analytical equivalent of denial.

**1.1M service lines had paid exceeding allowed at line level.** This nets out substantially at the procedure-code level, which is itself informative: aggregate reporting hides line-level payment anomalies that only surface when you validate at the grain the data actually arrives in.

---

## Data

[CMS Data Entrepreneurs' Synthetic Public Use File (DE-SynPUF)](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files) — Sample 1, covering a 5% random sample of 2008 Medicare beneficiaries with claims from 2008 through 2010.

| Source | Bronze rows |
|---|---|
| Beneficiary summary (3 annual files) | 343,644 |
| Inpatient claims | 66,773 |
| Outpatient claims | 790,790 |
| Carrier (professional) claims | 4,741,335 |

Synthetic data, so no PHI exposure — but structured exactly like real CMS claim files, including the wide-format encoding that makes them awkward to work with.

**Scope note:** DE-SynPUF provides claim records with payment amounts attached, not raw X12 837/835 transactions. The reconciliation logic here matches production claims work; the file format does not. No EDI segment parsing is involved.

---

## Architecture

```
CSV → UC volume → bronze → silver → validate → governance → gold → validate → dashboard
                Auto Loader  typed &            masking &   business
                             unpivoted          filters     aggregates
```

**Bronze** — Auto Loader ingestion with checkpointing and schema evolution. All columns land as strings with provenance metadata appended. Bronze interprets nothing, so it stays the ground truth when a downstream number looks wrong. Re-running is idempotent.

**Silver** — typed, deduplicated, restructured:
- Beneficiary dimension deduplicated across three annual files, with a derived chronic condition count from 11 flag columns
- Inpatient and outpatient claims normalized into a unified header at 846,520 rows, with Part A and Part B deductible/coinsurance mapped to their respective source columns
- **Carrier service line unpivot** — CMS stores up to 13 service lines as repeated column groups (`LINE_NCH_PMT_AMT_1` through `_13`, and five more groups alongside). Reshaping 145 wide columns into a long service-line grain is the core transformation in this pipeline.

**Gold** — 2,133 procedure codes with payment rate and zero-pay rate, 4,480 provider/claim-type combinations, and a monthly trend series.

---

## Data quality

Validation runs twice: between silver and gold, then again after the marts are built. Results log to a persistent `dq_audit` table with severity levels. Critical failures raise and halt the pipeline; warnings log and continue.

### Silver checks

| Check | Severity |
|---|---|
| Claim ID uniqueness | critical |
| Referential integrity (claims → beneficiary) | critical |
| Service dates ordered | critical |
| Payment amounts non-negative | critical |
| No service dated after beneficiary death | warn |
| Paid within allowed amount | warn |
| HCPCS code format | warn |
| Zero-pay rate within plausible band | warn |

### Gold checks

| Check | Severity |
|---|---|
| Mart grain — no join fan-out | critical |
| Gold totals reconcile to silver | critical |
| Claim counts reconcile to header | critical |
| Payment rate within sane bounds | critical |
| Variance identity holds | critical |
| Minimum-volume filter applied | critical |
| Overpayment detection | warn |
| Monthly series continuity | warn |

### Severity is a design decision, not a formality

The first version of the gold validation treated any payment rate above 100% as a critical failure, and the pipeline halted. Investigation showed the rates were correct — Medicare genuinely paid more than the allowed amount on those codes.

That's a business finding, not a broken pipeline. The check was split into three:

- **critical** — the metric is outside sane bounds (negative, or above 200%), indicating a computation error
- **warn** — allowed amount missing, so the rate can't be computed
- **warn** — paid exceeds allowed, which is real and worth reporting

A validation framework that can't distinguish "the pipeline is broken" from "the data is surprising" will either block on legitimate findings or be tuned until it catches nothing.

---

## Governance

PHI protection uses Unity Catalog column masks, row filters, and a de-identified view. Beneficiary identifiers are masked to the last four characters for users outside the `phi_readers` group; the de-identified view hashes identifiers and applies HIPAA Safe Harbor age banding, with all ages above 89 aggregated to a single `90+` band.

### Masking at the wrong layer breaks the pipeline

The column mask was initially applied to `silver.beneficiary`. Every subsequent referential integrity check failed — all 846,520 claims reported as orphans.

The cause: the pipeline itself runs without PHI read access, so it saw masked values (`XXXX45D1`) on one side of the join and real identifiers (`00013D2EFD8E45D1`) on the other. Zero overlap. The mask was working correctly; it was in the wrong place.

Masking belongs at the presentation boundary, not in the middle of a pipeline where joins happen. Moving it to the gold layer and the de-identified view fixed the joins while preserving the protection where it matters — at the point humans query the data.

This is the kind of thing that only surfaces by building it. The protection and the pipeline both worked in isolation; the interaction was the problem.

---

## Repository

```
01_bronze_ingest       Auto Loader ingestion, schema evolution, provenance
02_silver_transform    typing, dedup, claim normalization, carrier unpivot
03_validate            silver-layer quality gate
04_governance          masking functions, row filters, de-identified view
05_gold_marts          procedure, provider, and monthly aggregates
06_validate_gold       reconciliation against silver, metric bounds
```

Orchestrated as a Databricks Job with sequential task dependencies, so a critical validation failure prevents downstream tables from rebuilding on bad data.

---

## Running it

Requires a Databricks workspace with Unity Catalog. Built and tested on Free Edition (serverless only).

1. Download DE-SynPUF Sample 1 from CMS and organize into one folder per source table
2. Create the catalog, schemas, and landing volume
3. Upload the folders to `/Volumes/claims/bronze/landing`
4. Run notebooks `01` through `06` in order, or trigger the job

Ingestion is idempotent — Auto Loader checkpoints mean re-running skips files already processed.

---

## Platform notes

Databricks Free Edition is serverless-only; classic compute isn't available and clusters can't be configured. Relevant constraints:

- Spark RDD APIs are unavailable — serverless runs on Spark Connect, DataFrame APIs only
- Python and SQL only; no Scala or R
- Unity Catalog volumes replace DBFS
- Daily compute quotas apply

None of these materially affected the work.

Delta time travel provides reproducibility — any table state can be queried by version or timestamp, and a bad transform rolled back.

---

## Built with

Databricks · PySpark · Delta Lake · Auto Loader · Unity Catalog · Databricks SQL · AI/BI Dashboards
