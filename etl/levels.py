"""What differs between product levels -- everything else in the pipeline is
shared. This replaces three separately maintained ibi flows with one
parameterized flow.

    level   landing tables                 staging maps on                  subflow
    serial  tmp_serial                     STM.src_product_id = product_id  tmp -> src_stats -> stg
    sku     tmp_sku -> prestg_sku          STM.style/color/size             tmp -> prestg -> src_stats -> stg
    style   tmp_style -> prestg_style      STM.style                        tmp -> prestg -> src_stats -> stg
"""
from dataclasses import dataclass

from etl.parsers import KEY_COLUMNS, OUTPUT_COLUMNS


@dataclass(frozen=True)
class Level:
    name: str
    tmp_table: str
    prestg_table: str | None     # None: the level has no pre-staging aggregation (serial)
    src_key_sql: str             # source product key as text, for stage.stg_sales.src_product_key
    stm_match_sql: str           # ON clause (besides client + level) against dim.product_stm m

    @property
    def key_columns(self) -> list[str]:
        return KEY_COLUMNS[self.name]

    @property
    def copy_columns(self) -> list[str]:
        return OUTPUT_COLUMNS[self.name]

    @property
    def source_table(self) -> str:
        """Where src_stats and staging read from: prestg if the level has one."""
        return self.prestg_table or self.tmp_table


LEVELS = {
    "serial": Level(
        name="serial",
        tmp_table="landing.tmp_serial",
        prestg_table=None,
        src_key_sql="s.product_id",
        stm_match_sql="m.src_product_id = s.product_id",
    ),
    "sku": Level(
        name="sku",
        tmp_table="landing.tmp_sku",
        prestg_table="landing.prestg_sku",
        src_key_sql="s.style || '|' || s.color || '|' || s.size",
        stm_match_sql="m.style = s.style AND m.color = s.color AND m.size = s.size",
    ),
    "style": Level(
        name="style",
        tmp_table="landing.tmp_style",
        prestg_table="landing.prestg_style",
        src_key_sql="s.style",
        stm_match_sql="m.style = s.style",
    ),
}
