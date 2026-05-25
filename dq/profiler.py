import json
import boto3
import pandas as pd

from io import BytesIO

from datetime import datetime
from zoneinfo import ZoneInfo


INDIA_TZ = ZoneInfo("Asia/Kolkata")


s3_client = boto3.client("s3")


def read_parquet_from_s3(
    bucket_name,
    object_key
):

    response = s3_client.get_object(

        Bucket=bucket_name,

        Key=object_key
    )

    parquet_data = response["Body"].read()

    df = pd.read_parquet(BytesIO(parquet_data))

    return df


def save_json_to_s3(
    data,
    bucket_name,
    object_key
):

    json_data = json.dumps(
        data,
        indent=4
    )

    s3_client.put_object(

        Bucket=bucket_name,

        Key=object_key,

        Body=json_data,

        ContentType="application/json"
    )

    print(
        f"Saved to s3://"
        f"{bucket_name}/"
        f"{object_key}"
    )


def generate_profile(
    bucket_name,
    parquet_key,
    dataset_name
):

    df = read_parquet_from_s3(

        bucket_name,

        parquet_key
    )

    profile = {}

    profile["dataset_name"] = (dataset_name )

    profile["source_s3_path"] = (

        f"s3://{bucket_name}/"
        f"{parquet_key}"
    )

    profile["profile_timestamp"] = str( datetime.now(INDIA_TZ)   )

    profile["row_count"] = int(len(df))

    profile["column_count"] = int(len(df.columns))

    profile["columns"] = list(df.columns )

    null_counts = {}

    for col in df.columns:

        null_counts[col] = int(df[col].isnull().sum())

    profile["null_counts"] = (null_counts )

    null_percentages = {}

    for col in df.columns:

        if len(df):

            null_percentage = round((df[col].isnull().sum()/len(df)) * 100,2)

        else:

            null_percentage = 0

        null_percentages[col] = (null_percentage)

    profile["null_percentages"] = (null_percentages)

    duplicate_count = int(df.duplicated().sum())

    profile["duplicate_count"] = (duplicate_count )

    distinct_counts = {}

    for col in df.columns:

        distinct_counts[col] = int(df[col].nunique() )

    profile["distinct_counts"] = (distinct_counts )

    schema = {}

    for col in df.columns:

        schema[col] = str( df[col].dtype)

    profile["schema"] = schema

    numeric_stats = {}

    numeric_columns = (df.select_dtypes(include=["number"] ).columns)

    for col in numeric_columns:

        column_all_null = ( df[col].isnull().all() )

        if column_all_null:

            mean_value = 0
            std_value = 0
            min_value = 0
            max_value = 0

        else:

            mean_value = float(df[col].mean())

            std_value = float(df[col].std())

            min_value = float(df[col].min() )

            max_value = float(df[col].max())

        numeric_stats[col] = {

            "mean": mean_value,

            "std": std_value,

            "min": min_value,

            "max": max_value
        }

    profile["numeric_stats"] = (numeric_stats)

    memory_usage_mb = round((df.memory_usage(deep=True).sum()/ (1024 * 1024)),2)

    profile["memory_usage_mb"] = (memory_usage_mb)

    empty_columns = []

    for col in df.columns:

        column_empty = (df[col].isnull().all())

        if column_empty:

            empty_columns.append(col)

    profile["empty_columns"] = (empty_columns)

    unique_columns = []

    for col in df.columns:

        unique_count = (df[col].nunique())

        total_rows = len(df)

        if unique_count == total_rows:

            unique_columns.append(col)

    profile["fully_unique_columns"] = (unique_columns)

    sample_records = (
        df.head(5)
        .astype(str)
        .to_dict(
            orient="records"
        )
    )

    profile["sample_records"] = (sample_records)

    has_nulls = any(
        value > 0
        for value in (
            null_counts.values()
        )
    )

    has_duplicates = (duplicate_count > 0)

    has_empty_columns = (len(empty_columns) > 0)

    is_empty_dataset = (len(df) == 0)

    profile["health_flags"] = {

        "has_nulls": has_nulls,

        "has_duplicates": (has_duplicates),

        "has_empty_columns": (has_empty_columns),

        "is_empty_dataset": (is_empty_dataset)
    }

    profile["summary"] = {

        "total_rows": int( len(df)),

        "total_columns": int(len(df.columns)),

        "total_duplicates": (duplicate_count),

        "total_empty_columns": (len(empty_columns)),

        "memory_usage_mb": (memory_usage_mb)
    }

    metrics_key = (

        f"dq-metrics/"
        f"{dataset_name}_profile.json"
    )

    save_json_to_s3(

        profile,

        bucket_name,

        metrics_key
    )

    return profile
