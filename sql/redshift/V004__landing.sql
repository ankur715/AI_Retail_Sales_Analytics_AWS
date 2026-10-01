-- Landing tables, one pair per product level. Shared by every client/retailer
-- feed at that level; each load only touches its own load_id, and the
-- previous load for the same client/retailer is cleared before COPY.
--
-- tmp_*    : output.csv exactly as parsed (the ibi "tmp" synonym target)
-- prestg_* : tmp aggregated to the feed's key columns (store rows rolled up)

CREATE TABLE IF NOT EXISTS landing.tmp_serial (
    load_id      VARCHAR(60)    NOT NULL,
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    product_id   VARCHAR(30)    NOT NULL,   -- the retailer's serial/UPC (text: keeps leading zeros)
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.tmp_sku (
    load_id      VARCHAR(60)    NOT NULL,
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    style        VARCHAR(20)    NOT NULL,
    color        VARCHAR(20)    NOT NULL,
    size         VARCHAR(10)    NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.prestg_sku (
    load_id      VARCHAR(60)    NOT NULL,
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    style        VARCHAR(20)    NOT NULL,
    color        VARCHAR(20)    NOT NULL,
    size         VARCHAR(10)    NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL,
    source_rows  INTEGER        NOT NULL     -- how many tmp rows were rolled up
);

CREATE TABLE IF NOT EXISTS landing.tmp_style (
    load_id      VARCHAR(60)    NOT NULL,
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    style        VARCHAR(20)    NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.prestg_style (
    load_id      VARCHAR(60)    NOT NULL,
    week_date    DATE           NOT NULL,
    client_id    VARCHAR(10)    NOT NULL,
    retailer_id  VARCHAR(10)    NOT NULL,
    style        VARCHAR(20)    NOT NULL,
    sales        DECIMAL(18,2)  NOT NULL,
    inventory    INTEGER        NOT NULL,
    source_rows  INTEGER        NOT NULL
);
