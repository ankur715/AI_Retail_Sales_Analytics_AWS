from collections import Counter

from data_gen.catalog import CLIENTS, UNMAPPED_SERIAL, UNMAPPED_SKU, UNMAPPED_STYLE
from etl.seed import pipeline_rows, product_rows


def test_stm_keys_are_unique():
    """A duplicate STM key would fan out the staging LEFT JOIN and double
    count sales -- the reconcile step would catch it, but the seed shouldn't
    produce one."""
    _, stm = product_rows()
    keys = Counter((r[0], r[1], r[2], r[3], r[4], r[5]) for r in stm)
    assert max(keys.values()) == 1


def test_every_stm_target_exists_in_dim_product():
    products, stm = product_rows()
    assert {r[6] for r in stm} <= {p[0] for p in products}


def test_deliberately_unmapped_keys_are_absent():
    _, stm = product_rows()
    for client in CLIENTS:
        assert not any(r[0] == client and r[2] == UNMAPPED_SERIAL[client] for r in stm)
        assert not any(r[0] == client and r[1] == "style" and r[3] == UNMAPPED_STYLE for r in stm)
    assert not any(r[1] == "sku" and tuple(r[3:6]) == UNMAPPED_SKU for r in stm)


def test_prod_only_gets_promoted_pipelines():
    dev, prod = pipeline_rows("dev"), pipeline_rows("prod")
    assert len(dev) == 9
    assert {p["pipeline_id"] for p in dev} - {p["pipeline_id"] for p in prod} == {"C103_R203"}
