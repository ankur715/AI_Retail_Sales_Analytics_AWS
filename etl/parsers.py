"""Parser: normalize raw retailer files (CSV, XLSX or API JSON) into the
standard output_<client>_<retailer>.csv that Redshift COPYs into the
landing tables. One run can take several files (see parse_files).

What it handles (see data_gen/generate.py for the raw layouts):
  - header spelling: "Week_Date", "Week Date", " week_date ", "weekEnding" -> week_date
  - a title row above the header (XLSX exports)
  - dates as MM/DD/YYYY, ISO, or Excel datetimes
  - "$1,234.50" money strings
  - UPC product ids with leading zeros (read as text, never as numbers)
  - summary/TOTAL rows and blank rows
  - files routed to the wrong pipeline (client/retailer mismatch)
  - feeds with no client/retailer column (a vendor-portal export is already
    one client): filled from the pipeline metadata, the authoritative values

Rows that can't be parsed go to a parse-reject file with a reason. Rows
OLDER than the load window are dropped and counted (a 12-week file in a
5-week automation run only restates 5 weeks). Rows NEWER than the window
fail the parse: loading the file would archive data that never reached
the fact table (e.g. a history run whose end_week is before the file's).

The output has a fixed column order per product level, prefixed with
load_id so every downstream step can scope itself to this load.
"""
import io
import json
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from etl.weeks import SATURDAY

# Standard columns per product level (the order output.csv is written in,
# and the column list of the COPY into landing.tmp_<level>).
OUTPUT_COLUMNS = {
    "serial": ["load_id", "week_date", "client_id", "retailer_id", "product_id", "sales", "inventory"],
    "sku":    ["load_id", "week_date", "client_id", "retailer_id", "style", "color", "size", "sales", "inventory"],
    "style":  ["load_id", "week_date", "client_id", "retailer_id", "style", "sales", "inventory"],
}
KEY_COLUMNS = {"serial": ["product_id"], "sku": ["style", "color", "size"], "style": ["style"]}

# Every spelling seen in a retailer feed -> standard name. Matching is on the
# header lower-cased with spaces/underscores/symbols removed.
SYNONYMS = {
    "weekdate": "week_date", "weekending": "week_date", "weekenddate": "week_date",
    "client": "client_id", "clientid": "client_id", "vendor": "client_id",
    "retailer": "retailer_id", "retailerid": "retailer_id",
    "productid": "product_id", "upc": "product_id", "serial": "product_id",
    "style": "style", "color": "color", "size": "size",
    "sales": "sales", "salesamount": "sales", "netsales": "sales",
    "inventory": "inventory", "inventoryunits": "inventory", "onhandunits": "inventory", "onhand": "inventory",
    "store": "store", "storenumber": "store",
}


class WindowError(ValueError):
    """The file holds weeks after the load window."""


@dataclass
class ParseResult:
    output: pd.DataFrame
    rejects: pd.DataFrame
    out_of_window: int = 0
    stats: dict = field(default_factory=dict)

    def output_csv(self) -> bytes:
        return self.output.to_csv(index=False).encode()

    def rejects_csv(self) -> bytes:
        return self.rejects.to_csv(index=False).encode()


def _norm(header) -> str:
    return "".join(ch for ch in str(header).lower() if ch.isalnum())


def read_raw(body: bytes, file_format: str) -> pd.DataFrame:
    """Raw bytes -> DataFrame of strings (dtype=str keeps UPC leading zeros)."""
    if file_format == "csv":
        return pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False)
    if file_format == "xlsx":
        # Find the header row: the first of the top rows that names a week column.
        grid = pd.read_excel(io.BytesIO(body), header=None, dtype=object)
        header_row = next((i for i in range(min(10, len(grid)))
                           if any(SYNONYMS.get(_norm(v)) == "week_date" for v in grid.iloc[i])), None)
        if header_row is None:
            raise ValueError("No header row with a week column in the first 10 rows")
        df = grid.iloc[header_row + 1:].copy()
        df.columns = list(grid.iloc[header_row])
        return df.map(lambda v: "" if pd.isna(v) else (v.date().isoformat() if hasattr(v, "date") else str(v)))
    if file_format == "json":
        records = json.loads(body)
        return pd.DataFrame(records, dtype=str).fillna("")
    raise ValueError(f"Unsupported file format {file_format!r}")


def _parse_dates(s: pd.Series) -> pd.Series:
    iso = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    us = pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")
    return iso.fillna(us).dt.date


def _parse_money(s: pd.Series) -> pd.Series:
    cleaned = s.str.replace(r"[$,\s]", "", regex=True).str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def parse(body: bytes, file_format: str, *, level: str, client_id: str, retailer_id: str,
          load_id: str, window: tuple[date, date]) -> ParseResult:
    raw = read_raw(body, file_format)
    raw.columns = [SYNONYMS.get(_norm(c), _norm(c)) for c in raw.columns]
    # Present in the file: validated against the metadata below. Absent: stamped from it.
    for col, value in (("client_id", client_id), ("retailer_id", retailer_id)):
        if col not in raw.columns:
            raw[col] = value

    needed = OUTPUT_COLUMNS[level][1:]   # everything but load_id
    missing = [c for c in needed if c not in raw.columns]
    if missing:
        raise ValueError(f"{level} file is missing columns {missing}; got {list(raw.columns)}")

    df = raw[needed].apply(lambda col: col.astype(str).str.strip())
    df = df[(df != "").any(axis=1)]   # fully blank rows are noise, not rejects

    df["week_date"] = _parse_dates(df["week_date"])
    df["sales"] = _parse_money(df["sales"])
    df["inventory"] = pd.to_numeric(df["inventory"].str.replace(",", ""), errors="coerce")
    for k in KEY_COLUMNS[level]:
        df[k] = df[k].str.upper()

    # First failing rule wins, so every reject carries exactly one reason.
    reason = pd.Series("", index=df.index)
    rules = [
        (df[KEY_COLUMNS[level]].eq("TOTAL").any(axis=1), "summary/total row"),
        (df["week_date"].isna(), "unparseable week_date"),
        (df["week_date"].map(lambda d: d is not None and not pd.isna(d) and d.weekday() != SATURDAY),
         "week_date is not a Saturday"),
        (df[KEY_COLUMNS[level]].eq("").any(axis=1), "missing product key"),
        (df["sales"].isna(), "unparseable sales"),
        (df["inventory"].isna(), "unparseable inventory"),
        (df["client_id"] != client_id, f"client is not {client_id}"),
        (df["retailer_id"] != retailer_id, f"retailer is not {retailer_id}"),
    ]
    for mask, why in rules:
        reason = reason.mask((reason == "") & mask.fillna(False).astype(bool), why)

    rejects = raw.loc[reason[reason != ""].index].assign(reject_reason=reason[reason != ""])
    good = df[reason == ""].copy()

    newer = good["week_date"] > window[1]
    if newer.any():
        raise WindowError(f"{int(newer.sum())} rows are after the window end {window[1]} "
                          f"(latest week {max(good.loc[newer, 'week_date'])}); widen end_week or load "
                          f"this file in automation mode -- refusing to drop newer data")
    in_window = good["week_date"] >= window[0]
    out_of_window = int((~in_window).sum())
    good = good[in_window]

    good["inventory"] = good["inventory"].round().astype("int64")
    good["sales"] = good["sales"].round(2)
    good.insert(0, "load_id", load_id)
    good = good[OUTPUT_COLUMNS[level]]

    return ParseResult(
        output=good.reset_index(drop=True),
        rejects=rejects.reset_index(drop=True),
        out_of_window=out_of_window,
        stats={"raw_rows": int(len(df)), "parsed_rows": int(len(good)), "parse_rejects": int(len(rejects)),
               "out_of_window": out_of_window, "sales": round(float(good["sales"].sum()), 2),
               "inventory": int(good["inventory"].sum())},
    )


def parse_files(files: list[tuple[str, date | None, bytes]], file_format: str, *, level: str, client_id: str,
                retailer_id: str, load_id: str, window: tuple[date, date]) -> ParseResult:
    """Parse every file waiting for this pipeline into ONE output.

    files: (name, file_week_end, body). When several weekly drops piled up,
    they overlap (each re-sends 5 weeks), so for every week_date only the
    rows of the newest file are kept -- the same end state as loading the
    files one by one in order, in a single load. Files with the SAME
    week_end are parts of one drop (R201_C101_20261003_part1.csv, _part2)
    and are unioned."""
    outputs, rejects, per_file = [], [], []
    for name, week_end, body in files:
        res = parse(body, file_format, level=level, client_id=client_id, retailer_id=retailer_id,
                    load_id=load_id, window=window)
        outputs.append(res.output.assign(_file=name, _file_week=week_end or date.min))
        rejects.append(res.rejects.assign(source_file=name))
        per_file.append({"file": name, "week_end": week_end.isoformat() if week_end else None, **res.stats})

    combined = pd.concat(outputs, ignore_index=True)
    newest = combined.groupby("week_date")["_file_week"].transform("max")
    keep = combined["_file_week"] == newest
    for f in per_file:   # rows of each file that survive (vs superseded by a newer drop)
        used = int((keep & (combined["_file"] == f["file"])).sum())
        f["superseded_rows"], f["rows_used"] = f["parsed_rows"] - used, used
    output = combined[keep].drop(columns=["_file", "_file_week"]).reset_index(drop=True)

    return ParseResult(
        output=output,
        rejects=pd.concat(rejects, ignore_index=True),
        out_of_window=sum(f["out_of_window"] for f in per_file),
        stats={"files": len(files), "raw_rows": sum(f["raw_rows"] for f in per_file),
               "parsed_rows": int(len(output)), "parse_rejects": sum(f["parse_rejects"] for f in per_file),
               "out_of_window": sum(f["out_of_window"] for f in per_file),
               "superseded_rows": sum(f["superseded_rows"] for f in per_file),
               "sales": round(float(output["sales"].sum()), 2), "inventory": int(output["inventory"].sum()),
               "per_file": per_file},
    )
