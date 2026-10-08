# Checking the pipeline in the AWS console

Sign in as `ankur-admin` and set the region (top right) to **US East (N. Virginia) us-east-1**.
Everything this project created is tagged `Project = retail-sales`.

## 1. S3: the data lake

**S3 → Buckets → `retail-sales-lake-<account-id>`**

| Open | What you should see |
|---|---|
| `dev/landing/<client>/<retailer>/` | Empty after a successful run. This is where retailers drop files and the sensor looks. |
| `dev/archive/C101/R201/` | Every processed file, renamed `<load_id>__R201_C101_<week>.csv`. The multi-file run shows three files with the same load_id prefix (`…20261010.csv`, `…20261017_part1.csv`, `…_part2.csv`). |
| `dev/parsed/C101/R201/<load_id>/` | `output_C101_R201.csv`: the parser output Redshift COPYed. Open it to see the standard columns with `load_id` first. |
| `dev/rejects/C101/R201/<load_id>/` | `unmapped_products.csv`: the serial number with no STM mapping (`010199999999`), with its weeks and sales. |
| `dev/rejects/C101/R203/<load_id>/` | `parse_rejects.csv`: Harbor Mart's `TOTAL` row, with `reject_reason = summary/total row`. |
| `dev/landing/C102/R202/` → archive | The API pipeline lands its JSON payload first (`R202_C102_<week>.json`), so API loads are replayable like files. |
| `prod/` | Doesn't exist yet. Nothing has been loaded in prod. |

Bucket settings tabs:
- **Properties:** versioning *Enabled*, default encryption *SSE-S3*.
- **Permissions:** Block public access *On*; the bucket policy `DenyInsecureTransport` (TLS only).
- **Management → Lifecycle rules:** `expire-dev-parsed` (30 d), `expire-dev-rejects` (90 d), the same for prod, and `expire-old-versions` (7 d).
- Turn on **Show versions** in a landing folder to see that archived files left a delete marker. The move is recoverable.

## 2. Redshift Serverless

**Amazon Redshift → Serverless dashboard**

- **Workgroup `retail-sales`:** status *Available*, base capacity *8 RPUs*, publicly accessible *On*.
  - Network and security shows the security group `retail-sales-redshift`, which allows port 5439 from your IP only.
  - **Limits** tab: a usage limit of *3 RPU-hours per day*, action *Turn off user queries*. This is the cost stop.
- **Namespace `retail-sales`:** databases `retail_dev` and `retail_prod`.
  - **Security and encryption:** IAM role `retail-sales-redshift-copy` set as the default (used by `COPY … IAM_ROLE default`).

The workgroup still shows *Available* when idle. Serverless bills only while queries run.

## 3. Query the data (Redshift Query Editor v2)

1. Workgroup `retail-sales` → **Query data** (opens Query Editor v2).
2. In the tree on the left, click `retail-sales` and choose **Database user name and password**:
   - Database: `retail_dev`
   - User name: `admin`
   - Password: `REDSHIFT_PASSWORD` in the project's `.env`

If the connection hangs, your public IP probably changed. The security group only allows the
IP in `infra/terraform/terraform.tfvars`. Update `allowed_cidr` and `terraform apply`.

Paste these one at a time:

```sql
-- Metadata: one row per feed = one DAG in Airflow
SELECT * FROM etl.pipeline_config ORDER BY pipeline_id;

-- Every run: window, files, src/stg/rej totals, fact inserted/updated
SELECT pipeline_id, load_type, status, window_start, window_end,
       src_rows, stg_rows, rej_rows, src_sales, stg_sales + rej_sales AS stg_plus_rej_sales,
       fact_inserted, fact_updated, started_at
FROM etl.v_load_reconciliation
ORDER BY started_at;

-- Per-file lineage: the 3-file run (older file superseded by the newer drop)
SELECT f.load_id, f.source_file, f.file_week_end, f.parsed_rows, f.rows_used, f.superseded_rows
FROM etl.load_files f
ORDER BY f.loaded_at DESC, f.source_file;

-- The control totals behind the reconcile gate, by week
SELECT load_id, week_date, source, row_count, sales, inventory
FROM etl.etl_stats
WHERE load_id = (SELECT MAX(load_id) FROM etl.load_audit WHERE pipeline_id = 'C101_R201')
ORDER BY week_date, source;

-- Fact coverage: 9 client/retailer pairs, weeks loaded, products, sales
SELECT client_name, retailer_name, MIN(week_date), MAX(week_date),
       COUNT(DISTINCT week_date) AS weeks, COUNT(DISTINCT product_id) AS products, SUM(sales) AS sales
FROM fact.v_weekly_sales
GROUP BY 1, 2 ORDER BY 1, 2;

-- Restatements: rows the rolling 5-week MERGE updated after first insert
SELECT week_date, COUNT(*) AS rows, SUM(CASE WHEN updated_at > inserted_at THEN 1 ELSE 0 END) AS restated
FROM fact.fact_sales
WHERE client_id = 'C101' AND retailer_id = 'R201'
GROUP BY 1 ORDER BY 1;

-- Rejected (unmapped) source keys in the latest staging load per pair
SELECT client_id, retailer_id, product_level, src_product_key, COUNT(*) AS rows, SUM(sales) AS sales
FROM stage.stg_sales
WHERE product_id IS NULL
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;

-- STM mapping: how each level's source key reaches a product_id
SELECT * FROM dim.product_stm WHERE client_id = 'C101' ORDER BY product_level, product_id LIMIT 50;
```

Then switch the database to `retail_prod` and run the first query: 8 pipelines (C103_R203 isn't
promoted). `fact.fact_sales` there is empty until the prod history loads are run.

## 4. IAM: least privilege

**IAM → Roles → `retail-sales-redshift-copy`:** the inline policy `read-parsed-output` allows only
`s3:GetObject` on `dev/parsed/*` and `prod/parsed/*`. Redshift can't read landing or archive files.

**IAM → Users → `retail-sales-pipeline`:** the inline policy `pipeline-s3-access` allows S3
get/put/delete on `dev/*` and `prod/*` in this bucket only. It has one access key, which the local
`retail-pipeline` AWS profile (and Airflow) uses.

## 5. Cost

- **Billing and Cost Management → Credits:** the remaining credit balance and expiry.
- **Bills** (or Cost Explorer, grouped by *Service*): Redshift Serverless and S3 for this month.
- **Budgets:** your existing `My Zero-Spend Budget` emails you as soon as anything isn't covered by credits.

## 6. Tear down (when you're done)

```bash
cd infra/terraform && terraform destroy
```

This removes the bucket (with its contents), the Redshift namespace and workgroup (both
databases), the IAM role, user and key, and the security group. Then remove the local profile
`[retail-pipeline]` from `~/.aws/credentials`.
