"""Create (or reset) the triage agent's read-only Redshift user in the current
environment's database: SELECT on exactly the tables in triage.readonly.TABLES,
nothing else, 30 s statement timeout. Run once per environment as the admin user:

    RETAIL_ENV=dev python -m triage.setup_reader

The password comes from TRIAGE_REDSHIFT_PASSWORD in .env. Writing the triage
note back to etl.load_audit is done by the ETL user, never by this one.
"""
from etl import config, redshift
from triage.readonly import TABLES


def main() -> None:
    user, pw = config.TRIAGE_REDSHIFT_USER, config.TRIAGE_REDSHIFT_PASSWORD
    if not pw:
        raise SystemExit("Set TRIAGE_REDSHIFT_PASSWORD in .env first")
    exists = redshift.fetch_all("SELECT 1 FROM pg_user WHERE usename = %(u)s;", {"u": user})
    schemas = sorted({t.split(".")[0] for t in TABLES})
    # user/table names come from code, never from data; the password goes through a parameter
    redshift.run([
        (f"ALTER USER {user} PASSWORD %(pw)s;" if exists else f"CREATE USER {user} PASSWORD %(pw)s;", {"pw": pw}),
        (f"ALTER USER {user} SET statement_timeout TO 30000;", None),
        *[(f"GRANT USAGE ON SCHEMA {s} TO {user};", None) for s in schemas],
        *[(f"GRANT SELECT ON {t} TO {user};", None) for t in TABLES],
    ])
    print(f"{user}: {'password reset' if exists else 'created'}, SELECT on {', '.join(TABLES)} in {config.REDSHIFT_DB}")


if __name__ == "__main__":
    main()
