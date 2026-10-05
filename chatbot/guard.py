"""Validate LLM-written SQL before it reaches Redshift.

Parsed with sqlglot (not keyword matching -- a column named updated_at must
not be rejected for containing "update"). Accepted only if it is:
  - exactly one statement, and a read-only query (SELECT / UNION / WITH)
  - reading only the views this question was routed to (chat.* allowlist);
    CTE names defined in the query itself are allowed
  - row-capped: a missing or larger LIMIT becomes MAX_ROWS

The database user is read-only on the chat schema anyway; this is the
second line of defence and gives Gemini a precise error to fix.
"""
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

MAX_ROWS = 500
WRITE_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
               exp.Command, exp.Grant, exp.TruncateTable, exp.Copy)


class UnsafeSQL(ValueError):
    pass


def validate(sql: str, allowed_views: list[str]) -> str:
    """Return the cleaned, row-capped SQL, or raise UnsafeSQL with the reason."""
    sql = sql.strip().strip("`").removeprefix("sql").strip().rstrip(";")
    try:
        statements = [s for s in sqlglot.parse(sql, read="redshift") if s is not None]
    except ParseError as e:
        raise UnsafeSQL(f"SQL does not parse: {e}") from e
    if len(statements) != 1:
        raise UnsafeSQL(f"expected exactly one statement, got {len(statements)}")
    tree = statements[0]

    if not isinstance(tree, (exp.Select, exp.Union)):
        raise UnsafeSQL(f"only SELECT queries are allowed, got {type(tree).__name__}")
    if any(isinstance(node, WRITE_NODES) for node in tree.walk()):
        raise UnsafeSQL("query contains a write or DDL operation")

    allowed = {v.lower() for v in allowed_views}
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        name = f"{table.db}.{table.name}".lower() if table.db else table.name.lower()
        if not table.db and name in cte_names:
            continue
        if name not in allowed:
            raise UnsafeSQL(f"table {name} is not allowed; use only: {', '.join(sorted(allowed))}")

    limit = tree.args.get("limit")
    current = limit.expression if limit else None
    if current is None or not (current.is_int and int(current.name) <= MAX_ROWS):
        tree.set("limit", exp.Limit(expression=exp.Literal.number(MAX_ROWS)))
    return tree.sql(dialect="redshift")
