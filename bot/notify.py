import os
import smtplib
from email.message import EmailMessage
from typing import Optional

import requests

from .contact_store import load_contact


def _env(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()


def _resend_send(to_addr: str, subject: str, body: str) -> Optional[str]:
    api_key = _env("RESEND_API_KEY")
    if not api_key:
        return "RESEND_API_KEY not set"
    from_addr = _env("RESEND_FROM") or _env("SMTP_FROM") or _env("SMTP_USER") or "onboarding@resend.dev"
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "from": from_addr,
                "to": [to_addr],
                "subject": subject,
                "text": body,
            },
            timeout=25,
        )
        if r.status_code >= 300:
            return f"resend error: HTTP {r.status_code}: {r.text[:300]}"
        return None
    except Exception as e:
        return f"resend error: {e}"


def send_email(subject: str, body: str) -> Optional[str]:
    """
    SMTP email notifier.

    Required env:
      - SMTP_HOST
      - SMTP_PORT (default 587)
      - SMTP_USER
      - SMTP_PASS
      - SIGNAL_EMAIL_TO (comma-separated ok)
    Optional:
      - SMTP_FROM (defaults to SMTP_USER)
      - SMTP_TLS ("1" default)
    """
    # Prefer stored contact; fall back to env.
    c = load_contact()
    to_raw = (c.email if c else "") or _env("SIGNAL_EMAIL_TO") or ""

    host = _env("SMTP_HOST")
    user = _env("SMTP_USER")
    pw = _env("SMTP_PASS")
    if not to_raw:
        return "no recipient configured (save contact or set SIGNAL_EMAIL_TO)"

    # Prefer Resend if configured.
    if _env("RESEND_API_KEY"):
        err = _resend_send(to_raw, subject, body)
        if err is None:
            return None
        # If Resend fails, fall back to SMTP if configured.

    if not host or not user or not pw:
        return "email not configured (RESEND_API_KEY or SMTP_HOST/SMTP_USER/SMTP_PASS)"

    port = int(_env("SMTP_PORT", "587") or "587")
    use_tls = _env("SMTP_TLS", "1").lower() not in ("0", "false", "no")
    from_addr = _env("SMTP_FROM", user) or user

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join([x.strip() for x in to_raw.split(",") if x.strip()])
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=25) as s:
            if use_tls:
                s.starttls()
            s.login(user, pw)
            s.send_message(msg)
        return None
    except Exception as e:
        return f"email error: {e}"


def send_twilio_sms(body: str) -> Optional[str]:
    """
    Twilio SMS notifier.

    Required env:
      - TWILIO_ACCOUNT_SID
      - TWILIO_AUTH_TOKEN
      - TWILIO_FROM
      - SIGNAL_SMS_TO
    """
    sid = _env("TWILIO_ACCOUNT_SID")
    token = _env("TWILIO_AUTH_TOKEN")
    from_num = _env("TWILIO_FROM")
    c = load_contact()
    to_num = (c.phone_e164 if c else "") or _env("SIGNAL_SMS_TO") or ""
    if not (sid and token and from_num and to_num):
        if not to_num:
            return "no SMS recipient configured (save contact or set SIGNAL_SMS_TO)"
        return "Twilio not configured (TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_FROM)"

    try:
        r = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
            auth=(sid, token),
            data={"From": from_num, "To": to_num, "Body": body},
            timeout=25,
        )
        if r.status_code >= 300:
            return f"twilio error: HTTP {r.status_code}: {r.text[:300]}"
        return None
    except Exception as e:
        return f"twilio error: {e}"

