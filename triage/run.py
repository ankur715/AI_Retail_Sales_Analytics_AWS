"""Run the triage agent on a failed load from the command line and watch it work:

    python -m triage.run C101_R201_20261008T120000            # print the diagnosis
    python -m triage.run C101_R201_20261008T120000 --write    # ...and save it to etl.load_audit

The pipeline, failed task and error come from the load's audit row (read-only).
Needs LLM_PROVIDER other than none, Bedrock model access, and the triage_reader user.
"""
import argparse
import logging

from etl import config
from triage import agent
from triage.readonly import ReadOnlySession


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("load_id")
    ap.add_argument("--task", default="unknown", help="the task that failed, if known")
    ap.add_argument("--write", action="store_true", help="save the note to etl.load_audit")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not agent.enabled():
        raise SystemExit("LLM_PROVIDER=none: the triage agent is turned off")
    session = ReadOnlySession()
    try:
        rows = session.query("load_audit", load_id=args.load_id)
    finally:
        session.close()
    if not rows:
        raise SystemExit(f"No audit row for {args.load_id}")
    audit = rows[0]
    print(f"Triage {args.load_id} ({audit['pipeline_id']}, status {audit['status']}) "
          f"with {agent.model_id()}, at most {config.TRIAGE_MAX_STEPS} steps / {config.TRIAGE_MAX_TOKENS} tokens\n",
          flush=True)   # before the agent's step log (stderr) starts

    run = agent.triage_failed_load if args.write else agent.triage
    result = run(args.load_id, audit["pipeline_id"], args.task, audit.get("error_message") or "unknown error")
    print("\n" + "=" * 72)
    print(result.note or f"(no note: {result.status})")
    print("=" * 72)
    print(f"status {result.status} | {result.steps} steps | {result.tokens} tokens | "
          f"{result.redshift_queries} Redshift queries | saved: {bool(args.write and result.note)}")


if __name__ == "__main__":
    main()
