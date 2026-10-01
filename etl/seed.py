"""Load the dimension tables and the pipeline metadata into the current
environment's database. Idempotent: each table is replaced in one
transaction, so re-running after editing config/pipeline_config.csv is
how a pipeline is added, paused or re-scheduled.

Promotion: retail_dev gets every row of config/pipeline_config.csv;
retail_prod only gets rows with promoted_to_prod=true. Flipping that flag
(through a reviewed PR) and re-seeding prod is how a feed goes live.

Usage: RETAIL_ENV=dev python -m etl.seed
"""
from data_gen.catalog import CLIENTS, RETAILERS, UNMAPPED_SKU, load_pipelines, skus, style_names
from etl import config, redshift


def pipeline_rows(env: str) -> list[dict]:
    return [p for p in load_pipelines() if env == "dev" or p["promoted_to_prod"]]


def product_rows() -> tuple[list[tuple], list[tuple]]:
    """(dim.product rows, dim.product_stm rows) for every client."""
    products, stm = [], []
    for client_id in CLIENTS:
        catalog = skus(client_id)
        for s in catalog:
            products.append((s.product_id, client_id, "sku", s.style, s.color, s.size, s.name, s.category, s.unit_price))
            stm.append((client_id, "serial", s.upc, None, None, None, s.product_id))
            stm.append((client_id, "sku", None, s.style, s.color, s.size, s.product_id))
        for style, name in style_names(client_id).items():
            price = next(s.unit_price for s in catalog if s.style == style)
            products.append((f"{client_id}-{style}", client_id, "style", style, None, None, name, catalog[0].category, price))
            stm.append((client_id, "style", None, style, None, None, f"{client_id}-{style}"))
    # Sanity: the deliberately unmapped SKU really is missing from the STM.
    assert not any(r[1] == "sku" and r[3:6] == UNMAPPED_SKU for r in stm)
    return products, stm


def main() -> None:
    products, stm = product_rows()
    pipelines = pipeline_rows(config.ENV)
    statements = [
        ("DELETE FROM dim.client;", None),
        *[("INSERT INTO dim.client VALUES (%s, %s, %s);", (cid, name, cat)) for cid, (name, cat, _) in CLIENTS.items()],
        ("DELETE FROM dim.retailer;", None),
        *[("INSERT INTO dim.retailer VALUES (%s, %s);", (rid, name)) for rid, name in RETAILERS.items()],
        ("DELETE FROM dim.product;", None),
        *[("INSERT INTO dim.product VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", p) for p in products],
        ("DELETE FROM dim.product_stm;", None),
        *[("INSERT INTO dim.product_stm VALUES (%s, %s, %s, %s, %s, %s, %s);", m) for m in stm],
        ("DELETE FROM etl.pipeline_config;", None),
        *[("""INSERT INTO etl.pipeline_config
                (pipeline_id, client_id, retailer_id, product_level, source_type, source_location,
                 file_format, schedule, lookback_weeks, is_active)
              VALUES (%(pipeline_id)s, %(client_id)s, %(retailer_id)s, %(product_level)s, %(source_type)s,
                      %(source_location)s, %(file_format)s, %(schedule)s, %(lookback_weeks)s, %(is_active)s);""", p)
          for p in pipelines],
    ]
    redshift.run(statements)
    print(f"{config.REDSHIFT_DB}: {len(CLIENTS)} clients, {len(RETAILERS)} retailers, "
          f"{len(products)} products, {len(stm)} STM rows, {len(pipelines)} pipelines")


if __name__ == "__main__":
    main()
