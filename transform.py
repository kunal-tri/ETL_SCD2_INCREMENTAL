import os
import pandas as pd
from datetime import datetime

# INPUT / OUTPUT PATHS


INPUT_DIR = "extracted_data"
OUTPUT_DIR = "transformed_parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# DATASET CONFIG

DATASETS = {
    "customer": "customer_extract.csv",
    "loan": "loan_extract.csv",
    "document": "document_extract.csv"
}

# TRANSFORMATION FUNCTION

def transform_dataset(dataset_name, file_name):

    print(f"PROCESSING DATASET: {dataset_name.upper()}")

    file_path = os.path.join(INPUT_DIR, file_name)

    # LOAD DATA

    df = pd.read_csv(file_path)

    print(f"Initial Row Count: {len(df)}")
    print(f"Initial Column Count: {len(df.columns)}")

    # CHECK NULL VALUES

    print("\nNull Values Before Cleaning:")
    print(df.isnull().sum())

    # FILL NULL VALUES WITH 0

    df = df.fillna(0)

    print("\nNull Values After Cleaning:")
    print(df.isnull().sum())

    # CHECK DUPLICATES

    duplicate_count = df.duplicated().sum()

    print(f"\nDuplicate Rows Found: {duplicate_count}")

    # DROP DUPLICATES

    df = df.drop_duplicates()

    print(f"Row Count After Duplicate Removal: {len(df)}")

    # ADD ETL TIMESTAMP

    current_timestamp = datetime.now()

    df["etl_timestamp"] = current_timestamp

    print("\nETL Timestamp Added")

    # CONVERT TO PARQUET

    parquet_output_path = os.path.join(
        OUTPUT_DIR,
        f"{dataset_name}.parquet"
    )

    df.to_parquet(
        parquet_output_path,
        index=False,
        engine="pyarrow"
    )

    print(f"\nParquet File Saved:")
    print(parquet_output_path)

    print(f"{dataset_name.upper()} TRANSFORMATION COMPLETED")

    return df


# MAIN TRANSFORMATION PIPELINE

def run_transformation_pipeline():

    print("\nSTARTING TRANSFORMATION PIPELINE\n")

    transformed_dataframes = {}

    for dataset_name, file_name in DATASETS.items():

        transformed_df = transform_dataset(
            dataset_name,
            file_name
        )

        transformed_dataframes[dataset_name] = transformed_df

    print("\nALL DATASETS TRANSFORMED SUCCESSFULLY\n")

    return transformed_dataframes
