"""Create (or reset) the chatbot's read-only Redshift user in the current
environment's database: SELECT on the chat schema only, a 30s statement
timeout. Run once per environment, as the admin user (from .env):

    RETAIL_ENV=dev python -m chatbot.setup_reader

The password comes from CHAT_REDSHIFT_PASSWORD in .env. Redshift users are
cluster-wide, so the CREATE runs once; the grants are per database.

Redshift checks two things when chat_reader queries a chat view: SELECT on
the view, and USAGE on the schemas the view reads from (fact, dim, etl).
USAGE alone only lets it *refer* to objects there -- it still has no SELECT
on any fact/dim/etl table, so `SELECT * FROM fact.fact_sales` is refused.
"""

from etl import config, redshift

UNDERLYING_SCHEMAS = ("fact", "dim", "etl")


def main() -> None:
    user, pw = config.CHAT_REDSHIFT_USER, config.CHAT_REDSHIFT_PASSWORD
    if not pw:
        raise SystemExit("Set CHAT_REDSHIFT_PASSWORD in .env first")
    exists = redshift.fetch_all("SELECT 1 FROM pg_user WHERE usename = %(u)s;", {"u": user})
    # user/schema names come from config, never from data; the password goes through a parameter
    redshift.run([
        (f"ALTER USER {user} PASSWORD %(pw)s;" if exists else f"CREATE USER {user} PASSWORD %(pw)s;", {"pw": pw}),
        (f"ALTER USER {user} SET statement_timeout TO 30000;", None),
        (f"GRANT USAGE ON SCHEMA chat TO {user};", None),
        *[(f"GRANT USAGE ON SCHEMA {schema} TO {user};", None) for schema in UNDERLYING_SCHEMAS],
        (f"GRANT SELECT ON ALL TABLES IN SCHEMA chat TO {user};", None),   # views count as tables here
        # views added by later migrations are readable without re-running this
        (f"ALTER DEFAULT PRIVILEGES IN SCHEMA chat GRANT SELECT ON TABLES TO {user};", None),
    ])
    print(f"{user}: {'password reset' if exists else 'created'}, SELECT on chat.* in {config.REDSHIFT_DB}")


if __name__ == "__main__":
    main()
