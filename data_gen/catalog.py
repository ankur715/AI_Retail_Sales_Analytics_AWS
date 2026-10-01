"""Synthetic clients, retailers and product catalog -- the single source of
truth for the dimension seed (etl.seed), the dummy source files
(data_gen.generate) and the mock retailer API (mock_api).

Each client (a brand) has 5 styles x 2 colors x 2 sizes = 20 SKUs. Every
SKU has a retailer-facing UPC (what serial-level feeds report) and an
internal product_id; every style also has a style-level product_id (what
style-level feeds map to).

A few source keys are deliberately NOT in the mapping table (STM), the way
a retailer starts selling an item before the brand has set it up. Those
rows flow through to rej_stats instead of the fact table.
"""
import csv
import hashlib
import random
from dataclasses import dataclass
from pathlib import Path

CONFIG_CSV = Path(__file__).resolve().parent.parent / "config" / "pipeline_config.csv"

CLIENTS = {
    "C101": ("Aurora Apparel", "Apparel", ["Crew Tee", "Pullover Hoodie", "Jogger", "Denim Jacket", "Polo"]),
    "C102": ("Bramble Footwear", "Footwear", ["Trail Runner", "Court Sneaker", "Chelsea Boot", "Slide", "Loafer"]),
    "C103": ("Cobalt Outdoor", "Outdoor", ["Rain Shell", "Fleece Vest", "Daypack", "Beanie", "Trek Pant"]),
}
RETAILERS = {
    "R201": "Northgate Department Stores",
    "R202": "Summit Outfitters",
    "R203": "Harbor Mart",
}
COLORS = ["BLK", "NVY"]
SIZES = ["M", "L"]

# Source keys with no STM row -> rejected (product_id IS NULL in staging).
UNMAPPED_UPC = {c: f"0{c[1:]}99999999" for c in CLIENTS}       # serial level
UNMAPPED_SKU = ("S03", "GRN", "M")                             # sku level: a color the brand never set up
UNMAPPED_STYLE = "S99"                                         # style level: a style not yet in the catalog


@dataclass(frozen=True)
class Sku:
    client_id: str
    style: str          # S01..S05
    color: str
    size: str
    upc: str            # 12-digit, leading zero -- must stay a string end to end
    product_id: str     # internal SKU-level product id
    style_product_id: str
    name: str
    category: str
    unit_price: float


def stable_rng(*parts) -> random.Random:
    """Deterministic RNG from any key. Python's hash() is salted per process,
    so it can't be used for reproducible data -- sha256 is."""
    seed = int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)
    return random.Random(seed)


def skus(client_id: str) -> list[Sku]:
    _, category, style_names = CLIENTS[client_id]
    out, n = [], 0
    for s, style_name in enumerate(style_names, start=1):
        style = f"S{s:02d}"
        price = round(stable_rng(client_id, style).uniform(20, 180), 2)
        for color in COLORS:
            for size in SIZES:
                n += 1
                out.append(Sku(
                    client_id=client_id, style=style, color=color, size=size,
                    upc=f"0{client_id[1:]}{n:08d}",
                    product_id=f"{client_id}-P{n:03d}",
                    style_product_id=f"{client_id}-{style}",
                    name=f"{style_name} {color} {size}",
                    category=category, unit_price=price,
                ))
    return out


def style_names(client_id: str) -> dict[str, str]:
    return {f"S{i:02d}": name for i, name in enumerate(CLIENTS[client_id][2], start=1)}


def load_pipelines() -> list[dict]:
    """The metadata seed (config/pipeline_config.csv) as a list of dicts."""
    with CONFIG_CSV.open(newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["lookback_weeks"] = int(r["lookback_weeks"])
        r["is_active"] = r["is_active"].lower() == "true"
        r["promoted_to_prod"] = r["promoted_to_prod"].lower() == "true"
    return rows
