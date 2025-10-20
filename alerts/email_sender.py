from __future__ import annotations

from typing import List
from aiosmtplib import SMTP
from email.message import EmailMessage

from settings.config import settings


async def send_markdown_email(to_email: str, subject: str, markdown_body: str) -> bool:
    if not settings.SMTP_HOST or not settings.SMTP_PORT or not settings.ALERTS_FROM_EMAIL:
        return False
    msg = EmailMessage()
    msg["From"] = settings.ALERTS_FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    # Simple plaintext; HTML templating can be added later
    msg.set_content(markdown_body)
    try:
        smtp = SMTP(hostname=settings.SMTP_HOST, port=settings.SMTP_PORT)
        await smtp.connect()
        if settings.SMTP_USER and settings.SMTP_PASS:
            await smtp.starttls()
            await smtp.login(settings.SMTP_USER, settings.SMTP_PASS)
        await smtp.send_message(msg)
        await smtp.quit()
        return True
    except Exception:
        return False


