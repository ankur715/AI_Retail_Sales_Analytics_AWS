-- Weekly sell-through by client, retailer and product. Upserted (MERGE) over
-- each run's window -- 5 weeks in automation, any range for history loads.
CREATE TABLE IF NOT EXISTS fact.fact_sales (
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    product_id   VARCHAR(30)    NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL,
    load_id      VARCHAR(60)    NOT NULL,   -- last load that wrote this row (lineage)
    inserted_at  TIMESTAMP      NOT NULL DEFAULT GETDATE(),
    updated_at   TIMESTAMP      NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (week_date, client_id, retailer_id, product_id)
)
DISTKEY (product_id)
COMPOUND SORTKEY (week_date, client_id, retailer_id);
