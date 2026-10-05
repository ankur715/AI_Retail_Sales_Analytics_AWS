"""The chatbot's metadata (config/chat_catalog.yaml) and the prompt text
built from it.

Two levels, so each Gemini call only carries what it needs:
  - routing_text():  one-line summary per view -> Gemini picks the views
  - detail_text(views): columns, known values and examples of ONLY those
    views -> Gemini writes the SQL against them
"""
from functools import lru_cache
from pathlib import Path

import yaml

CATALOG_PATH = Path(__file__).resolve().parent.parent / "config" / "chat_catalog.yaml"


@lru_cache(maxsize=1)
def load() -> dict:
    return yaml.safe_load(CATALOG_PATH.read_text())


def view_names() -> list[str]:
    return list(load()["views"])


def routing_text() -> str:
    lines = [f"- {name}: {v['summary']}" for name, v in load()["views"].items()]
    return "\n".join(lines)


def known_value_columns(views: list[str]) -> dict[str, list[str]]:
    """{view: [columns whose distinct values belong in the prompt]}"""
    return {v: load()["views"][v].get("known_values", []) for v in views}


def detail_text(views: list[str], known_values: dict[str, list[str]]) -> str:
    """Prompt section for the selected views only.

    known_values: {"chat.v_sales_by_week.brand": ["Aurora Apparel", ...], ...}"""
    cat = load()
    parts = ["Business rules:", *[f"- {r}" for r in cat["business_rules"]], ""]
    for name in views:
        v = cat["views"][name]
        parts += [f"View {name} ({v['grain']}):"]
        parts += [f"  {col}: {desc}" for col, desc in v["columns"].items()]
        for col in v.get("known_values", []):
            values = known_values.get(f"{name}.{col}")
            if values:
                parts.append(f"  known values of {col}: {', '.join(map(str, values))}")
        parts += [f"  example: {ex['question']}\n    SQL: {ex['sql']}" for ex in v.get("examples", [])]
        parts.append("")
    return "\n".join(parts)
