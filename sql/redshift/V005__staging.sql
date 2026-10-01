-- Every product level converges here: week, client, retailer, product_id,
-- metrics. product_id is NULL when the source key had no STM match -- those
-- rows are the rejects (rej_stats) and never reach the fact table.
CREATE TABLE IF NOT EXISTS stage.stg_sales (
    load_id          VARCHAR(60)    NOT NULL,
    week_date        DATE           NOT NULL,
    client_id        VARCHAR(10)    NOT NULL,
    retailer_id      VARCHAR(10)    NOT NULL,
    product_level    VARCHAR(10)    NOT NULL,
    src_product_key  VARCHAR(80)    NOT NULL,   -- the source key as received, e.g. "S03|GRN|M"
    product_id       VARCHAR(30),               -- NULL = unmapped -> rejected
    sales            DECIMAL(18,2)  NOT NULL,
    inventory        INTEGER        NOT NULL
);
