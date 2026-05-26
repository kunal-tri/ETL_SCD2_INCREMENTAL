import json
import requests
import streamlit as st
import smtplib

from email.mime.text import MIMEText

from email.mime.multipart import MIMEMultipart
from dq.profiler import (
    generate_profile
)

from dq.context_builder import (
    build_llm_context,
    save_context_to_s3
)

from dq.llm_api import (
    analyze_dataset_from_s3
)

from dq.remediation_engine import (
    remediate_dataset
)

from dq.dq_audit_log import (
    save_audit_log
)


SLACK_WEBHOOK_URL = st.secrets[
    "SLACK_WEBHOOK_URL"
]

def send_slack_alert(

    dataset_name,

    llm_response,

    remediation_result,

    audit_log_key
):

    severity = llm_response.get(
        "severity",
        "UNKNOWN"
    )

    summary = llm_response.get(
        "summary",
        "No summary"
    )

    recommendation = (
        llm_response.get(
            "recommendation",
            "No recommendation"
        )
    )

    issues = llm_response.get(
        "issues",
        []
    )

    fixes = llm_response.get(
        "recommended_fixes",
        []
    )

    remediation_applied = (
        remediation_result.get(
            "remediation_applied",
            False
        )
    )

    remediation_log = (
        remediation_result.get(
            "log",
            []
        )
    )

    issues_text = "\n".join(

        [
            f"- {issue}"

            for issue in issues
        ]
    )

    fixes_text = "\n".join(

        [
            (
                f"- "
                f"{fix.get('action')} "
                f"on "
                f"{fix.get('column')}"
            )

            for fix in fixes
        ]
    )

    remediation_text = "\n".join(

        [
            str(log)

            for log in remediation_log
        ]
    )

    slack_message = {

        "text": (

            f"*AI DATA QUALITY ALERT*\n\n"

            f"*Dataset:* "
            f"{dataset_name}\n\n"

            f"*Severity:* "
            f"{severity}\n\n"

            f"*Issues Detected:*\n"
            f"{issues_text}\n\n"

            f"*AI Recommended Fixes:*\n"
            f"{fixes_text}\n\n"

            f"*Remediation Applied:* "
            f"{remediation_applied}\n\n"

            f"*Remediation Log:*\n"
            f"{remediation_text}\n\n"

            f"*Audit Log:*\n"
            f"{audit_log_key}\n\n"

            f"*Summary:*\n"
            f"{summary}\n\n"

            f"*Recommendation:*\n"
            f"{recommendation}"
        )
    }

    try:

        response = requests.post(

        SLACK_WEBHOOK_URL,

        json=slack_message,

        timeout=10
    )

        print(
            f"\nSlack Status Code: "
            f"{response.status_code}"
            )

        print(
                f"\nSlack Response: "
                f"{response.text}"
            )

        if response.status_code == 200:

            print("Slack alert sent")

        else:

            print(

            f"Slack Error: "
            f"{response.status_code}"
        )

    except Exception as e:

        print(
        f"\nSlack Exception: "
        f"{e}"
    )

    if response.status_code == 200:

        print("Slack alert sent")

    else:

        print(

            f"Slack Error: "
            f"{response.status_code} "
            f"{response.text}"
        )


def send_email_alert(

    dataset_name,

    llm_response,

    remediation_result,

    audit_log_key
):

    severity = llm_response.get(
        "severity",
        "UNKNOWN"
    )

    summary = llm_response.get(
        "summary",
        "No summary"
    )

    recommendation = (
        llm_response.get(
            "recommendation",
            "No recommendation"
        )
    )

    issues = llm_response.get(
        "issues",
        []
    )

    fixes = llm_response.get(
        "recommended_fixes",
        []
    )

    remediation_applied = (
        remediation_result.get(
            "remediation_applied",
            False
        )
    )

    remediation_log = (
        remediation_result.get(
            "log",
            []
        )
    )

    issues_text = "\n".join(

        [
            f"- {issue}"

            for issue in issues
        ]
    )

    fixes_text = "\n".join(

        [
            (
                f"- "
                f"{fix.get('action')} "
                f"on "
                f"{fix.get('column')}"
            )

            for fix in fixes
        ]
    )

    remediation_text = "\n".join(

        [
            str(log)

            for log in remediation_log
        ]
    )

    email_body = f"""

AI DATA QUALITY ALERT

Dataset:
{dataset_name}

Severity:
{severity}

Issues:
{issues_text}

AI Recommended Fixes:
{fixes_text}

Remediation Applied:
{remediation_applied}

Remediation Log:
{remediation_text}

Audit Log:
{audit_log_key}

Summary:
{summary}

Recommendation:
{recommendation}

"""

    try:

        sender = st.secrets["EMAIL_SENDER"]

        password = st.secrets["EMAIL_PASSWORD"]

        receiver = st.secrets["EMAIL_RECEIVER"]

        msg = MIMEMultipart()

        msg["From"] = sender

        msg["To"] = receiver

        msg["Subject"] = ("AI Data Quality Alert")

        msg.attach(MIMEText(email_body,"plain"))

        server = smtplib.SMTP("smtp.gmail.com",587)

        server.starttls()

        server.login(
            sender,
            password
        )

        server.sendmail(
            sender,
            receiver,
            msg.as_string()
        )

        server.quit()

        print("Email alert sent")

    except Exception as e:

        print(f"Email failed: {e}")


def run_ai_dq(

    bucket_name,

    parquet_key,

    dataset_name
):

    print(
        f"\nRunning AI DQ for "
        f"{dataset_name}\n"
    )

    # STEP 1 — GENERATE PROFILE

    profile = generate_profile(

        bucket_name,

        parquet_key,

        dataset_name
    )

    print("\nProfile Generated\n")

    # STEP 2 — BUILD CONTEXT

    context = build_llm_context(profile)

    context_key = (

        f"dq-contexts/"
        f"{dataset_name}_context.json"
    )

    save_context_to_s3(

        context,

        bucket_name,

        context_key
    )

    print("\nContext Stored in S3\n")

    # STEP 3 — GROQ ANALYSIS

    response_key = (

        f"dq-responses/"
        f"{dataset_name}_response.json"
    )

    llm_response = (
        analyze_dataset_from_s3(

            bucket_name,

            context_key,

            response_key
        )
    )

    print("\nAI RESPONSE:\n")

    print(
        json.dumps(
            llm_response,
            indent=4
        )
    )

    # STEP 4 — SAFE REMEDIATION

    print(
        "\nStarting Safe "
        "Remediation...\n"
    )

    remediation_result = (

        remediate_dataset(

            bucket_name,

            parquet_key,

            llm_response
        )
    )

    print("\nRemediation Result:\n")

    print(
        json.dumps(
            remediation_result,
            indent=4
        )
    )

    # STEP 5 — AUDIT LOGGING

    audit_log_key = (
        save_audit_log(

            bucket_name,

            dataset_name,

            llm_response,

            remediation_result
        )
    )

    print("\nAudit Log Saved:\n")

    print(audit_log_key)

    # STEP 6 — EMAIL/SLACK ALERT
    send_slack_alert(

        dataset_name,

        llm_response,

        remediation_result,

        audit_log_key
    )

    send_email_alert(

        dataset_name,

        llm_response,

        remediation_result,

        audit_log_key
    )

    print(
        "\nAI DQ Pipeline "
        "Completed\n"
    )

    return {

        "dataset_name":
        dataset_name,

        "llm_response":
        llm_response,

        "remediation_result":
        remediation_result,

        "audit_log_key":
        audit_log_key
    }
