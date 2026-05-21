

import os
import boto3
import pandas as pd
from io import BytesIO, StringIO


# AWS CONFIGURATION

import streamlit as st

AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = st.secrets["AWS_REGION"]
BUCKET_NAME = st.secrets["S3_BUCKET"]

# S3 object paths
CUSTOMER_FILE = "folder/nbfc_customer_data.csv"
LOAN_FILE = "folder/nbfc_loan_data.csv"
DOCUMENT_FILE = "folder/nbfc_document_data.csv"

# Local output directory
LOCAL_OUTPUT_DIR = "extracted_data"

os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)


# CREATE S3 CLIENT

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# =========================================================
# GENERIC EXTRACT FUNCTION
# =========================================================

def extract_csv_from_s3(bucket_name, file_key):
    """
    Extract CSV file from S3 into Pandas DataFrame
    """

    print(f"Reading file from S3: {file_key}")

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )

    csv_content = response["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(csv_content))

    print(f"Rows Extracted: {len(df)}")

    return df


# OPTIONAL PARQUET READER

def extract_parquet_from_s3(bucket_name, file_key):
    """
    Extract Parquet file from S3
    """

    print(f"Reading parquet file: {file_key}")

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )

    parquet_data = BytesIO(response["Body"].read())

    df = pd.read_parquet(parquet_data)

    print(f"Rows Extracted: {len(df)}")

    return df


# EXTRACT PIPELINE

def run_extract_pipeline():

    print("STARTING S3 EXTRACTION PIPELINE")

    # CUSTOMER DATA
    

    customer_df = extract_csv_from_s3(
        BUCKET_NAME,
        CUSTOMER_FILE
    )

    customer_df.to_csv(
        f"{LOCAL_OUTPUT_DIR}/customer_extract.csv",
        index=False
    )

    print("Customer data extracted successfully")

    # LOAN DATA

    loan_df = extract_csv_from_s3(
        BUCKET_NAME,
        LOAN_FILE
    )

    loan_df.to_csv(
        f"{LOCAL_OUTPUT_DIR}/loan_extract.csv",
        index=False
    )

    print("Loan data extracted successfully")

    # DOCUMENT DATA

    document_df = extract_csv_from_s3(
        BUCKET_NAME,
        DOCUMENT_FILE
    )

    document_df.to_csv(
        f"{LOCAL_OUTPUT_DIR}/document_extract.csv",
        index=False
    )

    print("Document data extracted successfully")

    print("EXTRACTION PIPELINE COMPLETED")
