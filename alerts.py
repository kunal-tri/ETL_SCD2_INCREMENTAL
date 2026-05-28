import requests
import smtplib

from email.mime.text import MIMEText


# SLACK ALERT

def send_slack_alert(message):

    try:

        webhook_url = "YOUR_SLACK_WEBHOOK_URL_2"

        payload = {
            "text": message
        }

        requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )

    except Exception as e:

        print(
            f"Slack Alert Failed: {e}"
        )


# EMAIL ALERT

def send_email_alert(
    subject,
    body
):

    try:

        sender_email = "YOUR_EMAIL@gmail.com"

        sender_password = "YOUR_APP_PASSWORD"

        receiver_email = "YOUR_EMAIL@gmail.com"

        msg = MIMEText(body)

        msg["Subject"] = subject

        msg["From"] = sender_email

        msg["To"] = receiver_email

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            sender_email,
            sender_password
        )

        server.sendmail(
            sender_email,
            receiver_email,
            msg.as_string()
        )

        server.quit()

    except Exception as e:

        print(
            f"Email Alert Failed: {e}"
        )
