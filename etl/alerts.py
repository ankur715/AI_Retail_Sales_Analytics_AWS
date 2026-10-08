"""Failure email: one message per final task failure, carrying the triage
agent's diagnosis and suggested fix, so the person on call gets the likely
cause with the alert instead of having to dig for it.

Sent from the DAG's failure callback AFTER the load is marked FAILED and the
triage agent has run. Like the agent, send_failure_email() never raises: an
SMTP problem is logged, and the task's own failure is what Airflow reports.

Plain SMTP with STARTTLS (Gmail: smtp.gmail.com:587 + an App Password),
configured from .env. Off unless SMTP_USER, SMTP_PASSWORD and ALERT_EMAIL are
all set -- CI and tests never send.

    python -m etl.alerts --test               # send a test email (checks the settings)
    python -m etl.alerts --load <load_id>     # email an existing failed load, with its saved triage note
"""
import argparse
import html
import logging
import smtplib
from email.message import EmailMessage

from etl import config

log = logging.getLogger(__name__)

ERROR_MAX_CHARS = 1500


def configured() -> bool:
    return bool(config.SMTP_USER and config.SMTP_PASSWORD and config.ALERT_EMAIL)


def _headline(note: str | None) -> str:
    """The DIAGNOSIS line of a triage note, for the subject."""
    for line in (note or "").splitlines():
        if line.upper().startswith("DIAGNOSIS:"):
            return line.split(":", 1)[1].strip()
    return ""


def build_message(*, pipeline_id: str | None, load_id: str | None, task_id: str, run_id: str | None,
                  error: str, log_url: str | None = None, triage_note: str | None = None,
                  triage_status: str | None = None) -> EmailMessage:
    what = pipeline_id or "a pipeline"
    headline = _headline(triage_note)
    subject = f"[retail-sales {config.ENV}] FAILED {what} at {task_id}"
    if headline:
        subject += f" -- {headline}"
    subject = subject[:200]

    error = error[:ERROR_MAX_CHARS]
    if triage_note:
        triage_text = triage_note
    elif triage_status == "skipped":
        triage_text = "Triage agent is off (LLM_PROVIDER=none)."
    elif load_id is None:
        triage_text = "No triage: the task failed before a load existed."
    else:
        triage_text = f"No triage note ({triage_status or 'not run'}). See the Airflow log."

    facts = [("Environment", config.ENV), ("Pipeline", pipeline_id or "-"), ("Load", load_id or "-"),
             ("Failed task", task_id), ("DAG run", run_id or "-")]
    text = "\n".join(f"{k}: {v}" for k, v in facts)
    text += f"\n\nError:\n{error}\n\nTriage (suggested fix needs human approval):\n{triage_text}\n"
    if log_url:
        text += f"\nAirflow log: {log_url}\n"
    if load_id:
        text += f"\nAudit row: SELECT * FROM etl.load_audit WHERE load_id = '{load_id}';\n"

    rows = "".join(f"<tr><td><b>{html.escape(k)}</b></td><td>{html.escape(str(v))}</td></tr>" for k, v in facts)
    body = (f"<h3>Load failed: {html.escape(what)} at {html.escape(task_id)}</h3><table>{rows}</table>"
            f"<h4>Triage (suggested fix needs human approval)</h4>"
            f"<pre style='white-space:pre-wrap'>{html.escape(triage_text)}</pre>"
            f"<h4>Error</h4><pre style='white-space:pre-wrap'>{html.escape(error)}</pre>")
    if log_url:
        body += f"<p><a href='{html.escape(log_url)}'>Open the Airflow log</a></p>"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.ALERT_FROM
    msg["To"] = config.ALERT_EMAIL
    msg.set_content(text)
    msg.add_alternative(body, subtype="html")
    return msg


def send(msg: EmailMessage) -> None:
    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(config.SMTP_USER, config.SMTP_PASSWORD)
        smtp.send_message(msg)


def send_failure_email(**kwargs) -> bool:
    """Build and send; True if sent. NEVER raises (same contract as the triage agent)."""
    if not configured():
        log.info("failure email skipped: SMTP_USER / SMTP_PASSWORD / ALERT_EMAIL not set")
        return False
    try:
        send(build_message(**kwargs))
        log.info("failure email sent to %s", config.ALERT_EMAIL)
        return True
    except Exception:
        log.exception("failure email could not be sent; the original failure stands")
        return False


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true", help="send a test email")
    group.add_argument("--load", metavar="LOAD_ID", help="email an existing failed load with its triage note")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not configured():
        raise SystemExit("Set SMTP_USER, SMTP_PASSWORD and ALERT_EMAIL in .env first")

    if args.test:
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = f"[retail-sales {config.ENV}] test email", config.ALERT_FROM, \
            config.ALERT_EMAIL
        msg.set_content("SMTP settings work: failure emails with triage notes will arrive here.")
        send(msg)
        print(f"test email sent to {config.ALERT_EMAIL}")
        return

    from etl import redshift
    rows = redshift.fetch_dicts("""SELECT load_id, pipeline_id, dag_run_id, error_message, triage_note
                                   FROM etl.load_audit WHERE load_id = %(l)s;""", {"l": args.load})
    if not rows:
        raise SystemExit(f"No audit row for {args.load}")
    r = rows[0]
    send(build_message(pipeline_id=r["pipeline_id"], load_id=r["load_id"], task_id="(from audit row)",
                       run_id=r["dag_run_id"], error=r["error_message"] or "", triage_note=r["triage_note"]))
    print(f"failure email for {args.load} sent to {config.ALERT_EMAIL}")


if __name__ == "__main__":
    main()
