"""Versioned, checksummed migrations for Redshift (Flyway-style, no extra tool).

- Files are sql/redshift/V<NNN>__<name>.sql, applied in order.
- Each file runs in its own transaction together with its row in
  etl.schema_migrations, so a failed migration leaves no trace.
- Editing an already-applied file is an error (checksum mismatch): schema
  changes go in a NEW migration. That's what keeps table evolution safe.

The target database comes from RETAIL_ENV (retail_dev / retail_prod). The
first run in an environment creates its database if it doesn't exist yet
(Terraform only creates retail_dev, as the namespace's initial database).

Usage: RETAIL_ENV=dev python -m etl.migrate [--dry-run]
"""
import hashlib
import re
import sys
from pathlib import Path

from etl import config
from etl.redshift import get_connection

# <project>/sql/redshift, located relative to this file (works from any folder).
SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "redshift"
# Valid names look like V001__schemas.sql -> version 1.
FILENAME = re.compile(r"^V(\d{3})__(\w+)\.sql$")

# The migrations table has to exist before we can check what's been applied.
BOOTSTRAP = """
CREATE SCHEMA IF NOT EXISTS etl;
CREATE TABLE IF NOT EXISTS etl.schema_migrations (
    version     INTEGER       NOT NULL,
    filename    VARCHAR(200)  NOT NULL,
    checksum    VARCHAR(64)   NOT NULL,
    applied_at  TIMESTAMP     NOT NULL DEFAULT GETDATE()
);
"""


def discover() -> list[tuple[int, Path, str]]:
    # Find every migration file, in name order, with a SHA-256 of its contents.
    found = []
    for path in sorted(SQL_DIR.glob("V*.sql")):
        m = FILENAME.match(path.name)
        if not m:
            raise ValueError(f"Bad migration filename: {path.name}")
        found.append((int(m.group(1)), path, hashlib.sha256(path.read_bytes()).hexdigest()))
    # Two files claiming the same version would make the order ambiguous.
    versions = [v for v, _, _ in found]
    if len(versions) != len(set(versions)):
        raise ValueError("Duplicate migration version numbers")
    return found


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into single statements. Redshift validates a
    multi-statement batch as a whole, so `CREATE SCHEMA x; CREATE TABLE x.t`
    sent together fails -- each statement must go on its own. Semicolons
    inside $$-quoted procedure bodies, 'strings' and -- comments don't count."""
    statements, buf, i = [], [], 0
    # Which kind of text we're currently inside (only one can be true at a time).
    in_dollar = in_quote = in_comment = False
    while i < len(sql):
        ch, two = sql[i], sql[i:i + 2]   # current char, and it plus the next one
        if in_comment:
            buf.append(ch)
            in_comment = ch != "\n"       # a -- comment ends at the end of the line
        elif in_dollar:
            if two == "$$":               # closing $$ of a procedure body
                buf.append(two)
                i += 1
                in_dollar = False
            else:
                buf.append(ch)            # anything (including ;) inside $$...$$ is body text
        elif in_quote:
            buf.append(ch)
            in_quote = ch != "'"          # closing quote ends the string
        elif two == "--":                 # start of a comment
            buf.append(two)
            i += 1
            in_comment = True
        elif two == "$$":                 # start of a procedure body
            buf.append(two)
            i += 1
            in_dollar = True
        elif ch == "'":                   # start of a string literal
            buf.append(ch)
            in_quote = True
        elif ch == ";":                   # a real statement terminator
            statements.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf).strip())  # whatever follows the last ;
    # Drop fragments that are only whitespace/comments.
    return [s for s in statements
            if any(line.strip() and not line.strip().startswith("--") for line in s.splitlines())]


def pending(applied: dict[int, str], migrations) -> list[tuple[int, Path, str]]:
    # applied = {version: checksum} from etl.schema_migrations.
    todo = []
    for version, path, checksum in migrations:
        if version in applied:
            # Already ran: its file must be byte-for-byte unchanged.
            if applied[version] != checksum:
                raise RuntimeError(
                    f"{path.name} was modified after being applied. "
                    "Never edit an applied migration -- add a new one."
                )
            continue
        todo.append((version, path, checksum))  # not applied yet -> run it
    return todo


def ensure_database(name: str) -> None:
    """CREATE DATABASE can't run inside a transaction or with IF NOT EXISTS
    on Redshift, so check the catalog first, from the namespace's initial
    database (retail_dev)."""
    conn = get_connection(dbname="retail_dev")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (name,))
            if cur.fetchone() is None:
                print(f"Creating database {name}")
                cur.execute(f"CREATE DATABASE {name};")   # name comes from config, never from data
    finally:
        conn.close()


def main(dry_run: bool = False) -> list[str]:
    print(f"Environment {config.ENV}: database {config.REDSHIFT_DB}")
    if not dry_run:
        ensure_database(config.REDSHIFT_DB)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Make sure the bookkeeping table exists, then read what's applied.
            for statement in split_statements(BOOTSTRAP):
                cur.execute(statement)
            conn.commit()
            cur.execute("SELECT version, checksum FROM etl.schema_migrations;")
            applied = dict(cur.fetchall())

        todo = pending(applied, discover())
        for version, path, checksum in todo:
            print(f"{'Would apply' if dry_run else 'Applying'} {path.name}")
            if dry_run:
                continue
            with conn.cursor() as cur:
                # Run each statement of the file, then record the file as applied --
                # all in one transaction, so it's all-or-nothing.
                for statement in split_statements(path.read_text()):
                    cur.execute(statement)
                cur.execute(
                    "INSERT INTO etl.schema_migrations (version, filename, checksum) VALUES (%s, %s, %s);",
                    (version, path.name, checksum),
                )
            conn.commit()
        if not todo:
            print("Schema up to date.")
        return [p.name for _, p, _ in todo]
    except Exception:
        conn.rollback()  # a failed migration leaves nothing half-applied
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # `python -m etl.migrate --dry-run` lists what would run without running it.
    main(dry_run="--dry-run" in sys.argv)
