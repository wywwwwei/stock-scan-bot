import os
from typing import List

# 邮件发送配置
EMAIL_ENABLED: bool = True
SMTP_SERVER: str = "smtp.gmail.com"
SMTP_PORT: int = 587
SENDER_EMAIL: str | None = os.getenv("EMAIL_NAME")
SENDER_PASSWORD: str | None = os.getenv("EMAIL_PASSWORD")
RECIPIENT_EMAIL: List[str] = [x for x in os.getenv("RECIPIENT_EMAIL").split(",") if x]
