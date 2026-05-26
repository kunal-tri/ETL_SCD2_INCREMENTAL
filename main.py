"""
MAIN ETL PIPELINE

This script orchestrates:

1. Extract Pipeline
2. Transform Pipeline
3. Load Pipeline
4. AI DQ Pipeline

Execution Flow:

S3 Raw CSV
    ↓
Extract
    ↓
Transform
    ↓
Load Parquet to S3
    ↓
AI DQ Analysis
    ↓
Slack Alerts
"""

import streamlit as st

from datetime import datetime
from zoneinfo import ZoneInfo

from extract import (
    run_extract_pipeline
)

from transform import (
    run_transformation_pipeline
)

from load import (
    run_load_pipeline
)

from dq.ai_dq_runner import (
    run_ai_dq
)


INDIA_TZ = ZoneInfo("Asia/Kolkata")


BUCKET_NAME = st.secrets["S3_BUCKET"]


CUSTOMER_PARQUET_KEY = (
    "output/parquet-data/"
    "customer.parquet"
)

LOAN_PARQUET_KEY = (
    "output/parquet-data/"
    "loan.parquet"
)

TRANSACTION_PARQUET_KEY = (
    "output/parquet-data/"
    "transaction.parquet"
)


def run_etl_pipeline():

    start_time = datetime.now(INDIA_TZ)

    print("STARTING END-TO-END " 
          "NBFC ETL PIPELINE"
    )

    try:

        print(
            "\n[STEP 1] "
            "EXTRACT PIPELINE STARTED\n"
        )

        run_extract_pipeline()

        print(
            "\n[STEP 1] "
            "EXTRACT PIPELINE COMPLETED\n"
        )

        print(
            "\n[STEP 2] "
            "TRANSFORMATION PIPELINE "
            "STARTED\n"
        )

        run_transformation_pipeline()

        print(
            "\n[STEP 2] "
            "TRANSFORMATION PIPELINE "
            "COMPLETED\n"
        )

        print(
            "\n[STEP 3] "
            "LOAD PIPELINE STARTED\n"
        )

        run_load_pipeline()

        print(
            "\n[STEP 3] "
            "LOAD PIPELINE COMPLETED\n"
        )

        print(
            "\n[STEP 4] "
            "AI DQ PIPELINE STARTED\n"
        )

        run_ai_dq(

            bucket_name=BUCKET_NAME,

            parquet_key=(CUSTOMER_PARQUET_KEY),

            dataset_name="customer"
        )

        run_ai_dq(

            bucket_name=BUCKET_NAME,

            parquet_key=(LOAN_PARQUET_KEY),

            dataset_name="loan"
        )

        run_ai_dq(

            bucket_name=BUCKET_NAME,

            parquet_key=(TRANSACTION_PARQUET_KEY),

            dataset_name="transaction"
        )

        print(
            "\n[STEP 4] "
            "AI DQ PIPELINE COMPLETED\n"
        )

        end_time = datetime.now(INDIA_TZ)

        total_time = (end_time - start_time)

        print(
            "NBFC ETL PIPELINE "
            "EXECUTED SUCCESSFULLY"
        )

        print(
            f"\nPipeline Start Time : "
            f"{start_time}"
        )

        print(
            f"Pipeline End Time   : "
            f"{end_time}"
        )

        print(
            f"Total Runtime       : "
            f"{total_time}"
        )

        print(
            "\nETL + AI DQ FLOW "
            "COMPLETED:"
        )

        print(
            '''
            S3 Raw CSV
                ↓
            Extract Pipeline
                ↓
            Transformation Pipeline
                ↓
            Parquet Conversion
                ↓
            Load to S3 Processed Zone
                ↓
            AI DQ Analysis
                ↓
            Slack Alerts
            '''
        )

    except Exception as e:

        print("\nETL PIPELINE FAILED")

        print(f"\nError: {e}")


if __name__ == "__main__":

    run_etl_pipeline()
