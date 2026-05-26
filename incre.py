import os
import json
import boto3
import pandas as pd
import streamlit as st
import requests

from dq.ai_dq_runner import (
    run_ai_dq
)
import smtplib

from email.mime.text import MIMEText

from email.mime.multipart import MIMEMultipart
from io import BytesIO
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent

ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)


AWS_REGION = os.getenv(
    "AWS_REGION",
    "ap-south-1"
)


TABLES = {

    "customer": {
        "business_key": "cust_id",
        "columns": [
            "cust_id",
            "Phone",
            "First Name",
            "Last Name",
            "Address",
            "Pincode"
        ],
        "s3_key":
        "output/parquet-data/customer.parquet"
    },

    "loan": {
        "business_key": "cust_id",
        "columns": [
            "cust_id",
            "loan_id",
            "Sanction_Loan",
            "Cibil_Score",
            "Start_Date",
            "Disbursed_Date",
            "Location",
            "Pincode",
            "Loan_Type",
            "Unit"
        ],

        "s3_key":
        "output/parquet-data/loan.parquet"
    },

    "document": {
        "business_key": "cust_id",
        "columns": [
            "cust_id",
            "KYC_Status",
            "Annual_Income",
            "Nationality",
            "Application_Type"
        ],

        "s3_key":
        "output/parquet-data/document.parquet"
    }
}


def send_email_alert(message):

    try:

        sender = st.secrets["EMAIL_SENDER"]

        password = st.secrets["EMAIL_PASSWORD"]

        receiver = st.secrets["EMAIL_RECEIVER"]

        msg = MIMEMultipart()

        msg["From"] = sender

        msg["To"] = receiver

        msg["Subject"] = ("ETL Pipeline Alert")

        msg.attach(MIMEText(message,"plain"))

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

        print(f"Email alert failed: {e}")

def send_slack_alert(message):

    try:

        SLACK_WEBHOOK_URL = st.secrets["SLACK_WEBHOOK_URL_2"]

        payload = {"text": message}

        requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10
        )

        print("Slack alert sent")

    except Exception as e:

        print(f"Slack alert failed: {e}")


def run_incremental_scd2_pipeline(
    source_df,
    table_name
):

    if table_name not in TABLES:

        raise ValueError(
            f"Invalid table name: "
            f"{table_name}"
        )

    AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]

    AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]

    BUCKET_NAME = st.secrets["S3_BUCKET"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    table_config = TABLES[table_name]

    BUSINESS_KEY = table_config["business_key"]

    COLUMNS = table_config["columns"]

    S3_KEY = table_config["s3_key"]

    source_df.columns = (source_df.columns.str.strip())

    source_df = (source_df.drop_duplicates())

    uploaded_columns = set(source_df.columns)

    expected_columns = set(COLUMNS)

    missing_columns = list(expected_columns-uploaded_columns)

    extra_columns = list(uploaded_columns - expected_columns)

    if missing_columns or extra_columns:

        anomaly_message = (

            f"\nANOMALY DETECTED\n\n"

            f"Wrong dataset uploaded.\n\n"

            f"Expected Table: "
            f"{table_name}\n\n"

            f"Expected Columns:\n"
            f"{list(expected_columns)}\n\n"

            f"Uploaded Columns:\n"
            f"{list(uploaded_columns)}\n\n"

            f"Missing Columns:\n"
            f"{missing_columns}\n\n"

            f"Unexpected Columns:\n"
            f"{extra_columns}\n\n"

            f"Upload blocked.\n"

            f"SCD2 processing stopped.\n"

            f"Parquet upload stopped.\n"

            f"Incremental merge stopped."
        )

        print(anomaly_message)

        send_email_alert(anomaly_message)
        
        send_slack_alert(anomaly_message)

        raise ValueError(anomaly_message)

    string_columns = [

        "cust_id",
        "Phone",
        "loan_id",
        "Pincode",
        "First Name",
        "Last Name",
        "Address",
        "Location",
        "Loan_Type",
        "Unit",
        "KYC_Status",
        "Nationality",
        "Application_Type"
    ]

    int_columns = [
        "Cibil_Score"
    ]

    float_columns = [
        "Sanction_Loan",
        "Annual_Income"
    ]

    date_columns = [
        "Start_Date",
        "Disbursed_Date"
    ]

    for col in string_columns:

        if col in source_df.columns:

            source_df[col] = (
                source_df[col]
                .astype(str)
                .fillna("")
                .str.strip()
            )

    for col in int_columns:

        if col in source_df.columns:

            source_df[col] = (
                pd.to_numeric(
                    source_df[col],
                    errors="coerce"
                )
                .fillna(0)
                .astype(int)
            )

    for col in float_columns:

        if col in source_df.columns:

            source_df[col] = (
                pd.to_numeric(
                    source_df[col],
                    errors="coerce"
                )
                .fillna(0.0)
            )

    for col in date_columns:

        if col in source_df.columns:

            source_df[col] = (
                pd.to_datetime(
                    source_df[col],
                    errors="coerce"
                )
            )

    try:

        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY
        )

        parquet_data = BytesIO(response["Body"].read())

        target_df = pd.read_parquet(parquet_data)

        print(
            "Existing parquet "
            "loaded from S3"
        )

    except Exception as e:

        print(
            f"No existing parquet found: "
            f"{e}"
        )

        target_df = pd.DataFrame(columns=COLUMNS)

    for col in string_columns:

        if col in target_df.columns:

            target_df[col] = (
                target_df[col]
                .astype(str)
                .fillna("")
                .str.strip()
            )

    scd_columns = {

        "start_date": pd.NaT,

        "end_date": pd.NaT,

        "is_current": "Y",

        "etl_timestamp": pd.NaT
    }

    for col, default_value in (scd_columns.items()):

        if col not in target_df.columns:

            target_df[col] = default_value

    final_df = target_df.copy()

    compare_columns = [

        col for col in COLUMNS

        if col != BUSINESS_KEY
    ]

    new_records_count = 0

    duplicate_records_count = 0

    historical_records_count = 0

    new_records = []

    duplicate_records = []

    historical_records = []

    for _, row in source_df.iterrows():

        new_record = row.to_dict()

        existing_rows = final_df[(final_df[BUSINESS_KEY]==new_record[BUSINESS_KEY]) &(final_df["is_current"]== "Y")]

        now = (datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None,microsecond=0))

        new_record["start_date"] = now

        new_record["end_date"] = pd.NaT

        new_record["is_current"] = "Y"

        new_record["etl_timestamp"] = now

        if existing_rows.empty:

            print(
                f"New record inserted: "
                f"{new_record[BUSINESS_KEY]}"
            )

            new_records_count += 1

            new_records.append(new_record)

            final_df = pd.concat(
                [
                    final_df,
                    pd.DataFrame(
                        [new_record]
                    )
                ],
                ignore_index=True
            )

        else:

            current_record = (existing_rows.iloc[0])

            exact_match = True

            for col in compare_columns:

                new_val = new_record.get(col)

                old_val = current_record.get(col)

                if pd.isna(new_val) and pd.isna(old_val):

                    continue

                if (str(new_val).strip()!=str(old_val).strip()):

                    exact_match = False
                    break

            if exact_match:

                print(f"Duplicate skipped: " f"{new_record[BUSINESS_KEY]}")

                duplicate_records_count += 1

                duplicate_records.append(new_record)

                continue

            changed = False

            for col in compare_columns:

                new_val = new_record.get(col)

                old_val = current_record.get(col)

                if pd.isna(new_val) and pd.isna(old_val):

                    continue

                if (str(new_val).strip()!=str(old_val).strip()):

                    changed = True

                    break

            if changed:

                print(
                    f"SCD2 update applied: "
                    f"{new_record[BUSINESS_KEY]}"
                )

                historical_records_count += 1

                historical_records.append(new_record)

                active_mask = (
                    (final_df[BUSINESS_KEY]==new_record[BUSINESS_KEY])
                    &
                    (final_df["is_current"] == "Y"))

                final_df.loc[active_mask,"is_current"] = "N"

                final_df.loc[active_mask,"end_date"] = now

                final_df = pd.concat(
                    [
                        final_df,
                        pd.DataFrame(
                            [new_record]
                        )
                    ],
                    ignore_index=True
                )

    for col in string_columns:

        if col in final_df.columns:

            final_df[col] = (
                final_df[col]
                .astype(str)
                .fillna("")
                .str.strip()
            )

    timestamp_columns = [
        "start_date",
        "end_date",
        "etl_timestamp"
    ]

    for col in timestamp_columns:

        if col in final_df.columns:

            final_df[col] = (
                pd.to_datetime(
                    final_df[col],
                    errors="coerce"
                )
                .dt.floor("ms")
                .astype("datetime64[ms]")
            )

    parquet_buffer = BytesIO()

    final_df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=parquet_buffer.getvalue()
    )

    try:

        print("\nStarting AI DQ Analysis\n")

        run_ai_dq(
            bucket_name=BUCKET_NAME,
            parquet_key=S3_KEY,
            dataset_name=table_name
        )

        print("\nAI DQ Completed\n")

    except Exception as ai_error:

        print(f"\nAI DQ Failed: "f"{ai_error}")

        send_email_alert(f"AI DQ Failed: {ai_error}")
        send_slack_alert(f"AI DQ Failed: {ai_error}")

    monitoring_summary = {
        "table_name": table_name,
        "run_timestamp":
        (
            datetime.now(
                ZoneInfo("Asia/Kolkata")
            )
            .replace(tzinfo=None,microsecond=0)
        ).strftime("%Y-%m-%d %H:%M:%S"),

        "total_rows_after_update":len(final_df),

        "new_records_count":new_records_count,

        "duplicate_records_count":duplicate_records_count,

        "historical_records_count":historical_records_count,

        "new_records":new_records,

        "duplicate_records":duplicate_records,

        "historical_records":historical_records
    }

    timestamp_folder = (
        datetime.now(
            ZoneInfo("Asia/Kolkata")
        )
        .replace(tzinfo=None,microsecond=0)
    ).strftime(
        "%Y%m%d_%H%M%S"
    )

    monitoring_key = (

        f"monitor/"

        f"{timestamp_folder}/"

        f"{table_name}_summary.json"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=monitoring_key,
        Body=json.dumps(
            monitoring_summary,
            default=str,
            indent=4
        )
    )

    print(f"Monitoring summary uploaded: "f"{monitoring_key}")

    return {
        "final_df": final_df,
        "monitoring_summary":monitoring_summary
    }
