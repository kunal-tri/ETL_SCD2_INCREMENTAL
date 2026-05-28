import requests
import smtplib
import streamlit as st

from email.mime.text import MIMEText


# SLACK ALERT
def send_slack_alert(message):

    try:

        webhook_url = st.secrets["SLACK_WEBHOOK_URL_2"]

        payload = {"text": message}

        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )

        if response.status_code != 200:

            raise Exception(
                f"Slack Error: "
                f"{response.status_code} "
                f"{response.text}"
            )

        print("Slack Alert Sent Successfully")

    except Exception as e:

        print(f"Slack Alert Failed: {e}")


# EMAIL ALERT
def send_email_alert(
    subject,
    body
):

    try:

        sender_email = st.secrets["EMAIL_SENDER"]

        sender_password = st.secrets["EMAIL_PASSWORD"]

        receiver_email = st.secrets["EMAIL_RECEIVER"]

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

        print("Email Alert Sent Successfully")

    except Exception as e:

        print(f"Email Alert Failed: {e}")
