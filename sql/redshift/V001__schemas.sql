-- One schema per layer. The same DDL runs in both retail_dev and retail_prod.
CREATE SCHEMA IF NOT EXISTS etl;      -- pipeline metadata, run audit, load stats
CREATE SCHEMA IF NOT EXISTS dim;      -- clients, retailers, products, source-to-target mapping (STM)
CREATE SCHEMA IF NOT EXISTS landing;  -- tmp_* (as parsed) and prestg_* (aggregated to key level)
CREATE SCHEMA IF NOT EXISTS stage;    -- mapped to product_id; one load per client/retailer at a time
CREATE SCHEMA IF NOT EXISTS fact;     -- the upserted weekly fact table
