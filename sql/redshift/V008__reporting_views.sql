-- Business-friendly view over the fact (names instead of ids). This is what
-- the analytics chatbot (next phase) will query.
CREATE OR REPLACE VIEW fact.v_weekly_sales AS
SELECT
    f.week_date,
    f.client_id,
    c.client_name,
    f.retailer_id,
    r.retailer_name,
    f.product_id,
    p.product_grain,
    p.style,
    p.color,
    p.size,
    p.product_name,
    p.category,
    f.sales,
    f.inventory
FROM fact.fact_sales f
JOIN dim.client   c ON c.client_id   = f.client_id
JOIN dim.retailer r ON r.retailer_id = f.retailer_id
JOIN dim.product  p ON p.product_id  = f.product_id;

-- Load health at a glance: one row per load, src vs stg vs rej.
CREATE OR REPLACE VIEW etl.v_load_reconciliation AS
SELECT
    a.pipeline_id,
    a.load_id,
    a.load_type,
    a.status,
    a.window_start,
    a.window_end,
    SUM(CASE WHEN s.source = 'src' THEN s.sales END) AS src_sales,
    SUM(CASE WHEN s.source = 'stg' THEN s.sales END) AS stg_sales,
    SUM(CASE WHEN s.source = 'rej' THEN s.sales END) AS rej_sales,
    SUM(CASE WHEN s.source = 'src' THEN s.row_count END) AS src_rows,
    SUM(CASE WHEN s.source = 'stg' THEN s.row_count END) AS stg_rows,
    SUM(CASE WHEN s.source = 'rej' THEN s.row_count END) AS rej_rows,
    a.fact_inserted,
    a.fact_updated,
    a.started_at,
    a.finished_at
FROM etl.load_audit a
LEFT JOIN etl.etl_stats s ON s.load_id = a.load_id
GROUP BY 1, 2, 3, 4, 5, 6, 13, 14, 15, 16;
