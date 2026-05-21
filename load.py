import os
import boto3

# AWS CONFIGURATION
import streamlit as st

AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = st.secrets["AWS_REGION"]
BUCKET_NAME = st.secrets["S3_BUCKET"]

# LOCAL PARQUET DIRECTORY

LOCAL_PARQUET_DIR = "transformed_parquet"

# NEW S3 TARGET FOLDER

S3_TARGET_FOLDER = "output/parquet-data"

# CREATE S3 CLIENT

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# UPLOAD FUNCTION

def upload_file_to_s3(local_file_path, bucket_name, s3_key):

    try:

        s3_client.upload_file(
            local_file_path,
            bucket_name,
            s3_key
        )

        print(f"Uploaded Successfully:")
        print(f"{local_file_path} -> s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Upload Failed: {e}")


# MAIN LOAD PIPELINE

def run_load_pipeline():

    print("STARTING PARQUET LOAD TO S3")

    # LOOP THROUGH ALL PARQUET FILES

    for file_name in os.listdir(LOCAL_PARQUET_DIR):

        if file_name.endswith(".parquet"):

            local_file_path = os.path.join(
                LOCAL_PARQUET_DIR,
                file_name
            )

            s3_key = f"{S3_TARGET_FOLDER}/{file_name}"

            upload_file_to_s3(
                local_file_path,
                BUCKET_NAME,
                s3_key
            )

    print("ALL PARQUET FILES UPLOADED")


