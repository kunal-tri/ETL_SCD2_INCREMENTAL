import json
import boto3
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


AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]

AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]

AWS_REGION = st.secrets["AWS_REGION"]

BUCKET_NAME = st.secrets["S3_BUCKET"]

SLACK_WEBHOOK_URL = st.secrets["SLACK_WEBHOOK_URL"]


s3_client = boto3.client(

    "s3",

    aws_access_key_id=(AWS_ACCESS_KEY),

    aws_secret_access_key=(AWS_SECRET_KEY),

    region_name=AWS_REGION
)


STATE_FILE = ("ai_dq_state.json")


PARQUET_PREFIX = ("output/parquet-data/")

CONTEXT_PREFIX = ("dq-contexts/")

RESPONSE_PREFIX = ("dq-responses/")


def load_previous_state():

    try:

        with open(STATE_FILE,"r") as file:

            return json.load(file)

    except Exception:

        return {}


def save_current_state(state):

    with open(STATE_FILE,"w") as file:

        json.dump(
            state,
            file,
            indent=4
        )


def list_parquet_files():

    response = (
        s3_client.list_objects_v2(

            Bucket=BUCKET_NAME,

            Prefix=PARQUET_PREFIX
        )
    )

    contents = response.get("Contents",[])

    parquet_files = []

    for obj in contents:

        key = obj["Key"]

        if key.endswith(".parquet"):

            parquet_files.append(obj)

    return parquet_files


def detect_changes(
    current_objects,
    previous_state
):

    changed_files = []

    current_state = {}

    for obj in current_objects:

        key = obj["Key"]

        last_modified = str(obj["LastModified"])

        current_state[key] = (last_modified)

        previous_modified = (previous_state.get(key))

        if previous_modified != (last_modified):

            changed_files.append(key)

    return (changed_files,current_state)


def send_slack_alert(
    dataset_name,
    llm_response
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

    issues = llm_response.get("issues",[])

    issues_text = "\n".join(

        [
            f"- {issue}"

            for issue in issues
        ]
    )

    slack_message = {

        "text": (

            f"*AI DATA QUALITY ALERT*\n\n"

            f"*Dataset:* "
            f"{dataset_name}\n\n"

            f"*Severity:* "
            f"{severity}\n\n"

            f"*Issues:*\n"
            f"{issues_text}\n\n"

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

        if response.status_code == 200:

            print("Slack alert sent")

        else:

            print(

                f"Slack Error: "
                f"{response.status_code} "
                f"{response.text}"
            )

    except Exception as e:

        print(
            f"Slack send failed: "
            f"{e}"
        )
def send_email_alert(
    dataset_name,
    llm_response
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

    issues = llm_response.get("issues",[])

    issues_text = "\n".join(

        [
            f"- {issue}"

            for issue in issues
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



def process_dataset(parquet_key):

    try:

        dataset_name = (

            parquet_key
            .split("/")[-1]
            .replace(".parquet", "")
        )

        print(
            f"\nProcessing "
            f"{dataset_name}"
        )

        profile = generate_profile(

            BUCKET_NAME,

            parquet_key,

            dataset_name
        )

        context = build_llm_context(profile)

        context_key = (

            f"{CONTEXT_PREFIX}"
            f"{dataset_name}_context.json"
        )

        save_context_to_s3(

            context,

            BUCKET_NAME,

            context_key
        )

        response_key = (

            f"{RESPONSE_PREFIX}"
            f"{dataset_name}_response.json"
        )

        llm_response = (
            analyze_dataset_from_s3(

                BUCKET_NAME,

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

        send_slack_alert(

            dataset_name,

            llm_response
        )
        send_email_alert(

            dataset_name,

            llm_response
        )
        
    except Exception as e:

        print(
            f"\nDataset Processing "
            f"Failed: {e}"
        )


def run_ai_dq_monitor():

    print("\nStarting AI DQ Monitor...\n")

    previous_state = (load_previous_state())

    current_objects = (list_parquet_files())

    changed_files, current_state = (

        detect_changes(

            current_objects,

            previous_state
        )
    )

    if not changed_files:

        print("No parquet changes detected")

    for parquet_key in (changed_files):

        process_dataset(parquet_key)

    save_current_state(current_state)

    print(
        "\nAI DQ Monitoring "
        "Completed\n"
    )


if __name__ == "__main__":

    run_ai_dq_monitor()
