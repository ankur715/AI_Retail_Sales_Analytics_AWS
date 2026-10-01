-- Internal product master: SKU-level products, plus one style-level product
-- per style (style-level retailer feeds can only be mapped that far).
CREATE TABLE IF NOT EXISTS dim.product (
    product_id     VARCHAR(30)    NOT NULL,
    client_id      VARCHAR(10)    NOT NULL,
    product_grain  VARCHAR(10)    NOT NULL,   -- sku | style
    style          VARCHAR(20)    NOT NULL,
    color          VARCHAR(20),               -- NULL for style-grain products
    size           VARCHAR(10),
    product_name   VARCHAR(200)   NOT NULL,
    category       VARCHAR(50),
    unit_price     DECIMAL(10,2),
    PRIMARY KEY (product_id)
)
DISTSTYLE ALL;   -- small dimension: a full copy on every node avoids redistribution in joins

-- STM (source-to-target mapping): how each retailer feed's product key maps
-- to an internal product_id. Staging LEFT JOINs to this; no match means the
-- row is rejected (rej_stats) instead of reaching the fact table.
CREATE TABLE IF NOT EXISTS dim.product_stm (
    client_id       VARCHAR(10)  NOT NULL,
    product_level   VARCHAR(10)  NOT NULL,   -- serial | sku | style (matches pipeline_config)
    src_product_id  VARCHAR(30),             -- serial level: retailer UPC / serial
    style           VARCHAR(20),             -- sku + style levels
    color           VARCHAR(20),             -- sku level
    size            VARCHAR(10),             -- sku level
    product_id      VARCHAR(30)  NOT NULL
)
DISTSTYLE ALL;
