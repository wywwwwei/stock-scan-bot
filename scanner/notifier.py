import smtplib
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailNotifier:
    """
    邮件通知器
    """

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        sender: str,
        password: str,
        receivers: List[str],
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender = sender
        self.password = password
        self.receivers = receivers

    def send(self, subject: str, content: str, format: str = "plain") -> None:
        """
        发送邮件（纯文本）

        :param subject: 邮件标题
        :param content: 邮件正文
        """
        if not self.receivers:
            print("[WARN] 邮件接收人为空，跳过发送")
            return

        msg = MIMEMultipart()
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.receivers)
        msg["Subject"] = subject
        msg.attach(MIMEText(content, format, "utf-8"))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.receivers, msg.as_string())

            print("[INFO] 邮件发送成功")

        except Exception as e:
            print(f"[ERROR] 邮件发送失败: {e}")
