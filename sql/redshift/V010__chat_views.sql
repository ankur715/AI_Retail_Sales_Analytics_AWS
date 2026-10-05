-- Curated views for the analytics chatbot. The chatbot's database user
-- (chat_reader, see chatbot/setup_reader.py) can read ONLY this schema --
-- never landing/stage/etl/fact directly. Names, not ids; metrics defined
-- here once; relative time pre-computed so the LLM never does date math.
--
-- "weeks_ago" is relative to the latest week loaded (0 = latest), not to
-- today: retailer data lags, and "last week" should mean the newest data.
CREATE SCHEMA IF NOT EXISTS chat;

-- One row per week in the fact table, with its calendar attributes.
CREATE OR REPLACE VIEW chat.v_calendar AS
SELECT
    w.week_date                                                    AS week_ending,
    DATEADD(day, -6, w.week_date)::DATE                            AS week_starting,
    EXTRACT(year FROM w.week_date)::INT                            AS year,
    EXTRACT(quarter FROM w.week_date)::INT                         AS quarter,
    EXTRACT(month FROM w.week_date)::INT                           AS month,
    TO_CHAR(w.week_date, 'Mon YYYY')                               AS month_name,
    (DATEDIFF(day, w.week_date, l.latest_week) / 7)::INT           AS weeks_ago
FROM (SELECT DISTINCT week_date FROM fact.fact_sales) w
CROSS JOIN (SELECT MAX(week_date) AS latest_week FROM fact.fact_sales) l;

-- Product-level detail: one row per week x client x retailer x product.
-- Each retailer reports at ONE grain: Harbor Mart rows are style-level
-- products (product_grain = 'style', no color/size); the others are SKUs.
CREATE OR REPLACE VIEW chat.v_weekly_sales AS
SELECT
    f.week_date                                                    AS week_ending,
    (DATEDIFF(day, f.week_date, l.latest_week) / 7)::INT           AS weeks_ago,
    c.client_name                                                  AS brand,
    r.retailer_name                                                AS retailer,
    p.category,
    p.product_id,
    p.product_name,
    p.product_grain,
    p.style,
    p.color,
    p.size,
    f.sales                                                        AS sales_usd,
    f.inventory                                                    AS inventory_units,
    (f.inventory = 0)                                              AS is_out_of_stock
FROM fact.fact_sales f
JOIN dim.client   c ON c.client_id   = f.client_id
JOIN dim.retailer r ON r.retailer_id = f.retailer_id
JOIN dim.product  p ON p.product_id  = f.product_id
CROSS JOIN (SELECT MAX(week_date) AS latest_week FROM fact.fact_sales) l;

-- Totals per week x brand x retailer, with week-over-week change.
CREATE OR REPLACE VIEW chat.v_sales_by_week AS
SELECT
    t.*,
    t.sales_usd - t.prev_week_sales_usd                            AS wow_change_usd,
    CASE WHEN t.prev_week_sales_usd > 0
         THEN ROUND(100.0 * (t.sales_usd - t.prev_week_sales_usd) / t.prev_week_sales_usd, 1)
    END                                                            AS wow_change_pct
FROM (
    SELECT
        s.week_ending,
        s.weeks_ago,
        s.brand,
        s.retailer,
        s.category,
        SUM(s.sales_usd)                                           AS sales_usd,
        SUM(s.inventory_units)                                     AS inventory_units,
        COUNT(*)                                                   AS products_reported,
        SUM(CASE WHEN s.is_out_of_stock THEN 1 ELSE 0 END)         AS products_out_of_stock,
        LAG(SUM(s.sales_usd)) OVER (PARTITION BY s.brand, s.retailer ORDER BY s.week_ending)
                                                                   AS prev_week_sales_usd
    FROM chat.v_weekly_sales s
    GROUP BY s.week_ending, s.weeks_ago, s.brand, s.retailer, s.category
) t;

-- How current the data is, per brand x retailer.
CREATE OR REPLACE VIEW chat.v_data_freshness AS
SELECT
    c.client_name                                                  AS brand,
    r.retailer_name                                                AS retailer,
    MIN(f.week_date)                                               AS first_week_loaded,
    MAX(f.week_date)                                               AS latest_week_loaded,
    COUNT(DISTINCT f.week_date)                                    AS weeks_loaded,
    MAX(a.finished_at)                                             AS last_successful_load_at
FROM fact.fact_sales f
JOIN dim.client   c ON c.client_id   = f.client_id
JOIN dim.retailer r ON r.retailer_id = f.retailer_id
LEFT JOIN etl.pipeline_config pc ON pc.client_id = f.client_id AND pc.retailer_id = f.retailer_id
LEFT JOIN etl.load_audit a       ON a.pipeline_id = pc.pipeline_id AND a.status = 'SUCCESS'
GROUP BY c.client_name, r.retailer_name;
