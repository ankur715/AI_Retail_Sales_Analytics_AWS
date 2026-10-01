# Retail Sales Analytics: Metadata-Driven Sell-Through Pipeline (AWS + Airflow)

Brands (clients) receive weekly sell-through data from their retailers:
sales and on-hand inventory by week. Each retailer sends it in its own
layout, at its own product level, by file drop or by API. This project loads
every client/retailer feed into a single weekly fact table on **Redshift
Serverless**, orchestrated by **Airflow**, with **S3** as the landing zone.

It is a rebuild of a legacy **ibi Data Migrator** setup. That setup had one
hand-built flow per client/retailer, each with its own hard-coded parameters
(week start/end, client_id, retailer_id, source/output/archive dirs). Here, a
**metadata table** drives everything: one row per feed, and a DAG factory
generates one DAG per row.

The analytics chatbot over the fact table (natural language → SQL, like
[Web_App](https://github.com/ankur715/Web_App)) is the next phase. The
`fact.v_weekly_sales` view is already shaped for it.

## Architecture

```
 Retailer feeds                        S3: retail-sales-lake-<acct>/<env>/
 ─────────────                         ──────────────────────────────────────────────
 R201 Northgate  CSV  serial level ─┐   landing/<client>/<retailer>/  files as received
 R202 Summit     XLSX sku level    ─┼─> parsed/.../<load_id>/output_<client>_<retailer>.csv  (what Redshift COPYs)
 R203 Harbor     CSV  style level  ─┘   rejects/.../<load_id>/        parse rejects + unmapped products
 C102@R202       REST API (mock) ────>  archive/<client>/<retailer>/  source files after a successful load
                                                     │ COPY (IAM role, parsed/ only)
                                                     ▼
 Redshift Serverless: retail_dev / retail_prod ───────────────────────────────────────
   etl.pipeline_config ─► DAG factory      dim.product, dim.product_stm (source-to-target map)
   landing.tmp_<level> ─► landing.prestg_<level> ─► stage.stg_sales ─MERGE─► fact.fact_sales
   etl.etl_stats (src / stg / rej)   etl.load_audit   etl.v_load_reconciliation   fact.v_weekly_sales
```

### One DAG per metadata row

`build_dag(row)` in `airflow/dags/retail_sales_dag_factory.py` is the only
DAG definition. Each metadata column switches one thing, and everything
else is shared code:

| Column | What it changes |
|---|---|
| `pipeline_id`, `client_id`, `retailer_id` | DAG id and tags; every SQL step is scoped to this pair and its `load_id`; parser validation (or stamping) of client/retailer |
| `product_level` | the subflow: `etl/levels.py` gives the landing tables, whether `prestg` exists, the key columns, and the STM join |
| `source_type` | `s3` → `wait_for_file` sensor on `source_location`; `api` → `extract_api` |
| `file_format` | the parser's reader: CSV, XLSX (header-row detection) or JSON |
| `schedule`, `lookback_weeks`, `is_active` | cron, the automation window, and whether the DAG exists at all |

```
retail_sales__C101_R201__serial   (s3, daily)
  wait_for_file ─► begin ─► parse ─► record_parse ─► serial_subflow ─────────────────────────────────────────────┐
                                     load_tmp ─► src_stats ─► load_staging ─► stg_stats ─┐       │
                                                                          └─► rej_stats ─┴► reconcile
                                                                                                 ▼
                                                                         merge_fact ─► archive ─► finish

retail_sales__C101_R202__sku      sku_subflow:   load_tmp ─► load_prestg ─► src_stats ─► ... (same)
retail_sales__C101_R203__style    style_subflow: load_tmp ─► load_prestg ─► src_stats ─► ... (same)
retail_sales__C102_R202__sku      (api, Sundays) begin ─► extract_api ─► parse ─► record_parse ─► sku_subflow ─► ...
```

| ibi Data Migrator | Here |
|---|---|
| Per-flow parameters (week start/end, client_id, retailer_id, source_dir, output_dir, archive_dir) | `etl.pipeline_config` row + `RETAIL_ENV` + optional `start_week`/`end_week` DAG params |
| Python parser → `output.csv`, read through a synonym | `etl/parsers.py` → `s3://…/parsed/<client>/<retailer>/<load_id>/output_<client>_<retailer>.csv`, `COPY` into `landing.tmp_<level>` |
| Load to tmp (serial) / prestg_sku / prestg_style | `load_tmp`, plus `load_prestg` for sku/style (rolls store rows up to the key columns) |
| src_stats / stg_stats / rej_stats | `etl.etl_stats` with `source` = `src` / `stg` / `rej`, by week, client and retailer |
| Load to staging: LEFT JOIN to the dimension STM | `stage.stg_sales`, LEFT JOIN `dim.product_stm`; `product_id IS NULL` = rejected |
| Merge staging to facts, last 5 weeks | Redshift `MERGE` into `fact.fact_sales` over the load window |
| *(new)* | `reconcile`: fails the run unless src = stg + rej (rows, sales, inventory) for every week |
| *(new)* | `etl.load_audit`: one row per run with window, files, parse stats, inserted/updated counts; `etl.load_files`: one row per source file |

## The three product levels

| Level | Source columns | Landing | Maps on (`dim.product_stm`) |
|---|---|---|---|
| serial | week_date, client, retailer, product_id, sales, inventory | `tmp_serial` | `src_product_id` (retailer UPC → internal SKU `product_id`) |
| sku (style-color-size) | week_date, retailer, client, style, color, size, sales, inventory | `tmp_sku` → `prestg_sku` | `style + color + size` → SKU `product_id` |
| style | week_date, retailer, client, style, sales, inventory | `tmp_style` → `prestg_style` | `style` → style-level `product_id` |

All three converge in `stage.stg_sales` (week, client, retailer, product_id,
metrics), so a single MERGE serves every feed.

## Metadata table

`config/pipeline_config.csv` is the reviewed seed for `etl.pipeline_config`:

| pipeline_id | client | retailer | product_level | source_type | file_format | schedule | lookback_weeks | promoted_to_prod |
|---|---|---|---|---|---|---|---|---|
| C101_R201 | C101 | R201 | serial | s3 | csv | `0 7 * * *` | 5 | ✓ |
| C101_R202 | C101 | R202 | sku | s3 | xlsx | `0 7 * * *` | 5 | ✓ |
| C102_R202 | C102 | R202 | sku | **api** | json | `0 8 * * 0` | 5 | ✓ |
| C103_R203 | C103 | R203 | style | s3 | csv | `0 7 * * *` | 5 | ✗ (dev only) |
| … 9 rows | | | | | | | | |

**Parallel pipelines and the `redshift` pool.** All feeds share the
landing, stage, stats and fact tables, and Redshift's serializable isolation
aborts concurrent writers (error 1023). So every task that writes to
Redshift runs in a 1-slot Airflow pool. S3 and API work (`wait_for_file`,
`extract_api`, `parse`, `archive`) runs in parallel.

When several pipelines wait for the slot, Airflow takes the highest
`priority_weight` first, then the oldest run. `weight_rule="upstream"`
gives later steps more weight, so a load that has started runs through
`merge_fact` before the next pipeline's `load_tmp` begins. This was
verified live: with three DAGs triggered together, one load ran tmp →
merge without interleaving while the other's API pull and parse ran
alongside it. `max_active_runs=1` keeps a pipeline from overlapping itself:
a manual run waits behind a scheduled run whose sensor is still polling.
The failure callback can't use a pool, so it retries on error 1023.

**Updating the metadata.** The CSV is the source of truth, reviewed like code:

1. Edit `config/pipeline_config.csv`: add a row for a new feed, or change
   `schedule`, `lookback_weeks`, `is_active` or `promoted_to_prod`. Open a
   PR to `dev`.
2. `RETAIL_ENV=dev python -m etl.seed --metadata-only` replaces
   `etl.pipeline_config` in `retail_dev`.
3. Trigger `retail_metadata_sync`, or wait for its 06:00 run. The snapshot
   refreshes, and within about 30 seconds Airflow shows the new or changed
   DAG. A pipeline whose `is_active` is false simply disappears; its run
   history is kept.
4. After the merge to `main`: run step 2 with `RETAIL_ENV=prod`, then sync
   in the prod Airflow.

Don't hand-edit `etl.pipeline_config` with SQL: the next seed overwrites it.

**The DAG factory never queries Redshift.** Airflow re-parses DAG files about
every 30 seconds, and each query would wake (and bill) the Serverless
workgroup. Instead, the `retail_metadata_sync` DAG copies
`etl.pipeline_config` into a local JSON snapshot once a day (or on demand),
and the factory reads that snapshot. Before the first sync, for example in
CI, it falls back to the seed CSV.

## Scheduling and the load window

- **S3 feeds run daily.** `wait_for_file` is a reschedule-mode sensor: it
  doesn't hold a worker while waiting, and it *skips* the run if no file
  lands within 8 hours. A file arrives once a week, so most days nothing
  happens and Redshift is never touched.
- **The API feed runs Sundays at 08:00**, after the Saturday week closes.
- **Week = week-ending Saturday.** Retailers restate recent weeks (returns,
  late POS uploads), so every weekly file re-sends the last 5 weeks, and each
  automation run upserts exactly that window:
  - *S3:* the 5 weeks ending at the week in the file name, e.g. `R201_C101_20261003.csv`.
  - *API:* the 5 weeks ending at the last completed Saturday.
- **History or backfill:** trigger the DAG with `{"start_week": "2026-07-11", "end_week": "2026-09-26"}`.
- **Several files in one run.** Everything waiting in landing/ (up to 20
  files) is loaded together:
  - a backlog of weekly drops (for example, the files for Oct 10 and Oct 17
    both waiting)
  - one drop split into parts (`R201_C101_20261017_part1.csv`, `_part2`)

  Parts of the same week are combined. Where drops overlap, the newest
  file's rows win each week. The window runs from the oldest file's 5 weeks
  to the newest file's week. The end state is exactly what loading the files
  one by one would produce, but in one load, one MERGE and one audit row.
  `etl.load_files` records what each file contributed, including the rows a
  newer file superseded.
- **Rows newer than the window fail the parse instead of being dropped.**
  Loading the file would otherwise archive data that never reached the fact
  table. (Found during the first Airflow run, see below.)

## Dev vs prod

The same code and DAGs run in both environments. `RETAIL_ENV` only changes
*where* data goes:

| | dev | prod |
|---|---|---|
| Redshift database | `retail_dev` | `retail_prod` (same namespace, isolated database) |
| S3 prefix | `dev/landing`, `dev/parsed`, … | `prod/landing`, … |
| Pipelines | every row of the seed | rows with `promoted_to_prod = true` |
| Airflow | `./start_airflow.sh` on :8080 | `RETAIL_ENV=prod ./start_airflow.sh` on :8081, with its own `AIRFLOW_HOME` |

**Promotion:** develop a new feed in dev, load its history with
`start_week`/`end_week`, and check `etl.v_load_reconciliation` and the
unmapped-products file. Then flip `promoted_to_prod` in a PR to `main`,
re-seed prod, and run the prod history load once. After that, the daily
schedule only upserts the rolling 5 weeks. Schema changes go through
versioned, checksummed migrations (`sql/redshift/V*.sql`) applied to dev
first. CI runs on `dev` and `main`.

## Dummy data

3 clients (Aurora Apparel, Bramble Footwear, Cobalt Outdoor) × 3 retailers.
Each client has 20 SKUs (5 styles × 2 colors × 2 sizes) and 12 weeks of
history plus one weekly drop. `data_gen/` produces each retailer's messy
layout:
- R201: `MM/DD/YYYY` dates, `"$1,234.50"` amounts, UPCs with leading zeros
- R202: XLSX with a title row above the header, one row per store
- R203: padded lower-case headers and a `TOTAL` row at the bottom
- API: camelCase JSON, cursor-paged

Values are a pure function of product, store, week and file date, so
restated weeks drift in a reproducible way. One key per level is
deliberately missing from the STM, so `rej_stats` always has something to
show.

## Setup

```bash
# 1. Infrastructure (S3, Redshift Serverless, IAM) -- ~5 min
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars          # your IP, a Redshift password
terraform init && terraform apply
aws configure set aws_access_key_id "$(terraform output -raw pipeline_access_key_id)" --profile retail-pipeline
aws configure set aws_secret_access_key "$(terraform output -raw pipeline_secret_access_key)" --profile retail-pipeline

# 2. Python (one venv for Airflow + the etl package)
cd ../..
python3.11 -m venv .venv
(cd airflow && ../.venv/bin/pip install -r requirements.txt)
cp .env.example .env                                   # paste the terraform outputs

# 3. Schema + seed, per environment
.venv/bin/python -m etl.migrate && .venv/bin/python -m etl.seed
RETAIL_ENV=prod .venv/bin/python -m etl.migrate && RETAIL_ENV=prod .venv/bin/python -m etl.seed

# 4. Mock retailer API (separate terminal)
.venv/bin/uvicorn mock_api.main:app --port 9100

# 5. Dummy files -> S3 landing (12-week history, then a weekly drop)
.venv/bin/python -m data_gen.generate --week-end 2026-09-26 --weeks 12 --upload
.venv/bin/python -m data_gen.generate --week-end 2026-10-03 --weeks 5 --upload

# 6. Airflow -> http://localhost:8080
.venv/bin/python -c "from etl import metadata; metadata.sync_snapshot()"
airflow/start_airflow.sh
```

To run one pipeline from the CLI without Airflow (same steps as its DAG):

```bash
.venv/bin/python -m etl.run C101_R201 --start-week 2026-07-11 --end-week 2026-09-26   # history
.venv/bin/python -m etl.run C101_R201                                                 # automation (all pending files)
```

## Tests

```bash
.venv/bin/pytest -q tests                  # 46 unit tests: parser layouts, multi-file precedence/parts, week windows, STM, SQL per level, S3 (moto), API paging
cd airflow && ../.venv/bin/pytest -q tests # 12 DAG integrity tests: one DAG per pipeline, subflow shape, reconcile gates the merge, every Redshift writer pooled, priority order
```

CI (`.github/workflows/ci.yml`) runs both suites plus `terraform fmt` and
`terraform validate` on every push to `dev`/`main` and on every PR.

## Verified on live AWS

`terraform apply` created all 15 resources. Migrations V001–V009 applied to
both `retail_dev` and `retail_prod`. Then all 9 dev pipelines loaded 12
weeks of history and one weekly drop, through the CLI and through Airflow,
including the API feed: **22 loads, all reconciled (src = stg + rej), 9 pairs ×
13 weeks in `fact.fact_sales`**. The weekly runs upserted exactly the 5-week
window, for example *20 inserted (new week), 80 updated (4 restated weeks)*,
and left older weeks untouched.

A multi-file run through Airflow loaded 3 files in one load: the Oct 10
drop plus the Oct 17 drop in two parts. Only Sep 12 came from the older
file; its 84 overlapping rows were superseded. The result was 40 fact rows
inserted and 80 updated, and the load reconciled.

**The first Airflow run surfaced one real bug.** Unpausing a DAG creates its
latest scheduled run. That run took a feed's history file as a 5-week
automation load. A manual history run (ending Sep 26) then picked up the next
file, which ran to Oct 3, and the parser silently dropped the Oct 3 rows
before the file was archived. Rows newer than the window now fail the parse,
and the runbook is: load history *before* unpausing a new feed.

Step-by-step console checks (S3, Redshift Query Editor, IAM, usage limit):
[docs/AWS_CHECK_GUIDE.md](docs/AWS_CHECK_GUIDE.md).

## Cost

Redshift Serverless bills only while queries run (8 RPU base, a 3 RPU-hour
daily usage limit that deactivates the workgroup if hit). S3 is cents.
Sensors only list S3, so idle days cost nothing on Redshift. `terraform
destroy` removes everything (`force_destroy` on the dummy-data bucket).

## Layout

```
config/pipeline_config.csv     metadata seed (reviewed in PRs)
etl/                           config, weeks, parser, level specs, steps, CLI runner, migrate, seed, metadata sync
sql/redshift/V*.sql            versioned migrations (schemas, metadata, dims, landing, staging, stats/audit, fact, views)
airflow/dags/                  DAG factory + metadata sync DAG
airflow/start_airflow.sh       local Airflow per environment
data_gen/                      synthetic catalog + retailer file layouts
mock_api/                      FastAPI retailer API (bearer token, cursor paging)
infra/terraform/               S3, IAM (COPY role + pipeline user), Redshift Serverless, usage limit
tests/, airflow/tests/         unit + DAG integrity tests
```
