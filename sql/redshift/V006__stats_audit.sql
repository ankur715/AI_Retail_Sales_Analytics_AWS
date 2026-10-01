-- Control totals per load and week. source = 'src' (landing, after parse),
-- 'stg' (mapped rows) or 'rej' (unmapped rows). The reconcile step requires
-- src = stg + rej for every week before anything is merged into the fact.
CREATE TABLE IF NOT EXISTS etl.etl_stats (
    load_id      VARCHAR(60)    NOT NULL,
    source       VARCHAR(5)     NOT NULL,   -- src | stg | rej
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    row_count    INTEGER        NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    BIGINT         NOT NULL,
    created_at   TIMESTAMP      NOT NULL DEFAULT GETDATE()
)
SORTKEY (load_id);

-- One row per pipeline run: what was loaded, from where, and how it ended.
CREATE TABLE IF NOT EXISTS etl.load_audit (
    load_id         VARCHAR(60)    NOT NULL,
    pipeline_id     VARCHAR(40)    NOT NULL,
    dag_run_id      VARCHAR(250),
    load_type       VARCHAR(12)    NOT NULL,   -- automation | history
    source_file     VARCHAR(500),
    window_start    DATE           NOT NULL,
    window_end      DATE           NOT NULL,
    status          VARCHAR(12)    NOT NULL,   -- RUNNING | SUCCESS | FAILED
    parse_stats     VARCHAR(1000),             -- JSON from the parser
    fact_inserted   INTEGER,
    fact_updated    INTEGER,
    error_message   VARCHAR(2000),
    started_at      TIMESTAMP      NOT NULL DEFAULT GETDATE(),
    finished_at     TIMESTAMP,
    PRIMARY KEY (load_id)
);
