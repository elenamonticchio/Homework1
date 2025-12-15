import os
import smtplib
from email.mime.text import MIMEText

def send_email(to_addr: str, subject: str, body: str):
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "1025"))
    user = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASS", "")
    mail_from = os.getenv("MAIL_FROM") or user or "noreply@example.com"

    if not host:
        raise RuntimeError("SMTP_HOST mancante")
    if not to_addr:
        raise RuntimeError("Destinatario mancante (to_addr)")

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_addr

    if port == 465:
        server = smtplib.SMTP_SSL(host, port)
    else:
        server = smtplib.SMTP(host, port)
        server.ehlo()

        if port == 587:
            server.starttls()
            server.ehlo()

    with server as s:
        if user and password:
            s.login(user, password)
        s.sendmail(mail_from, [to_addr], msg.as_string())
