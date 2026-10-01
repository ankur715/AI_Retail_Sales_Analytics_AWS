-- Replaces the per-pipeline parameters of the old ibi Data Migrator flows
-- (client_id, retailer_id, source_dir, ...): one row per client/retailer feed,
-- and the Airflow DAG factory builds one DAG per active row.
CREATE TABLE IF NOT EXISTS etl.pipeline_config (
    pipeline_id      VARCHAR(40)   NOT NULL,   -- <client_id>_<retailer_id>, also the DAG id suffix
    client_id        VARCHAR(10)   NOT NULL,
    retailer_id      VARCHAR(10)   NOT NULL,
    product_level    VARCHAR(10)   NOT NULL,   -- serial | sku | style -> which subflow the DAG runs
    source_type      VARCHAR(10)   NOT NULL,   -- s3 | api
    source_location  VARCHAR(200)  NOT NULL,   -- landing prefix (s3) or endpoint path (api)
    file_format      VARCHAR(10)   NOT NULL,   -- csv | xlsx | json
    schedule         VARCHAR(40)   NOT NULL,   -- cron, in Airflow's timezone (UTC)
    lookback_weeks   SMALLINT      NOT NULL DEFAULT 5,
    is_active        BOOLEAN       NOT NULL DEFAULT TRUE,
    updated_at       TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (pipeline_id)
);

CREATE TABLE IF NOT EXISTS dim.client (
    client_id    VARCHAR(10)   NOT NULL,
    client_name  VARCHAR(100)  NOT NULL,
    category     VARCHAR(50),
    PRIMARY KEY (client_id)
);

CREATE TABLE IF NOT EXISTS dim.retailer (
    retailer_id    VARCHAR(10)   NOT NULL,
    retailer_name  VARCHAR(100)  NOT NULL,
    PRIMARY KEY (retailer_id)
);
