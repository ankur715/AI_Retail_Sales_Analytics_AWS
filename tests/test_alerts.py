"""Failure email: message content and the never-raise contract. smtplib is faked."""
import smtplib

import pytest

from etl import alerts, config

NOTE = ("DIAGNOSIS: A source key matches more than one STM row.\nEVIDENCE:\n- rows src 21 vs stg + rej 22\n"
        "SUGGESTED FIX (needs human approval): remove the duplicate row.\nCONFIDENCE: high")
KW = dict(pipeline_id="C101_R201", load_id="C101_R201_X", task_id="serial_subflow.reconcile",
          run_id="scheduled__2026-10-08", error="ReconciliationError('src != stg + rej')",
          log_url="http://localhost:8080/log?a=1&b=2")


@pytest.fixture
def smtp_on(monkeypatch):
    monkeypatch.setattr(config, "SMTP_USER", "sender@example.com")
    monkeypatch.setattr(config, "SMTP_PASSWORD", "app-password")
    monkeypatch.setattr(config, "ALERT_EMAIL", "oncall@example.com")
    monkeypatch.setattr(config, "ALERT_FROM", "sender@example.com")


class FakeSMTP:
    instances = []

    def __init__(self, host, port, timeout):
        self.host, self.port, self.calls = host, port, []
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def starttls(self):
        self.calls.append("starttls")

    def login(self, user, password):
        self.calls.append(("login", user))

    def send_message(self, msg):
        self.calls.append(("send", msg))


def test_subject_carries_the_diagnosis_and_body_the_whole_note(smtp_on):
    msg = alerts.build_message(**KW, triage_note=NOTE, triage_status="answered")
    assert msg["Subject"] == ("[retail-sales dev] FAILED C101_R201 at serial_subflow.reconcile -- "
                              "A source key matches more than one STM row.")
    assert msg["To"] == "oncall@example.com" and msg["From"] == "sender@example.com"
    text = msg.get_body(("plain",)).get_content()
    assert "SUGGESTED FIX (needs human approval): remove the duplicate row." in text
    assert "Load: C101_R201_X" in text and "Airflow log: http://localhost:8080/log?a=1&b=2" in text
    html = msg.get_body(("html",)).get_content()
    assert "rows src 21 vs stg + rej 22" in html and "a=1&amp;b=2" in html     # escaped in HTML


@pytest.mark.parametrize("load_id,status,expected", [
    (None, None, "No triage: the task failed before a load existed."),
    ("L1", "skipped", "Triage agent is off (LLM_PROVIDER=none)."),
    ("L1", "error", "No triage note (error). See the Airflow log."),
])
def test_without_a_note_the_email_says_why(smtp_on, load_id, status, expected):
    msg = alerts.build_message(**{**KW, "load_id": load_id}, triage_note=None, triage_status=status)
    assert expected in msg.get_body(("plain",)).get_content()
    assert msg["Subject"].endswith("at serial_subflow.reconcile")            # no headline without a note


def test_long_errors_are_truncated(smtp_on):
    msg = alerts.build_message(**{**KW, "error": "x" * 10_000})
    assert msg.get_body(("plain",)).get_content().count("x") <= alerts.ERROR_MAX_CHARS + 10


def test_sends_over_starttls_with_login(smtp_on, monkeypatch):
    FakeSMTP.instances.clear()
    monkeypatch.setattr(alerts.smtplib, "SMTP", FakeSMTP)
    assert alerts.send_failure_email(**KW, triage_note=NOTE) is True
    smtp = FakeSMTP.instances[0]
    assert (smtp.host, smtp.port) == ("smtp.gmail.com", 587)
    assert smtp.calls[0] == "starttls" and smtp.calls[1] == ("login", "sender@example.com")
    assert smtp.calls[2][0] == "send"


def test_unconfigured_is_skipped_without_connecting(monkeypatch):
    monkeypatch.setattr(alerts.smtplib, "SMTP", lambda *a, **k: pytest.fail("must not connect"))
    assert not alerts.configured()                        # the suite clears SMTP settings
    assert alerts.send_failure_email(**KW) is False


def test_smtp_errors_never_escape(smtp_on, monkeypatch):
    def refuse(*a, **k):
        raise smtplib.SMTPAuthenticationError(535, b"bad credentials")

    monkeypatch.setattr(alerts.smtplib, "SMTP", refuse)
    assert alerts.send_failure_email(**KW, triage_note=NOTE) is False
