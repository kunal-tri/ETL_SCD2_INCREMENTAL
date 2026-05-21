import boto3
import pandas as pd

from io import BytesIO


s3_client = boto3.client(
    "s3"
)


def read_parquet_from_s3(

    bucket_name,

    parquet_key
):

    response = s3_client.get_object(

        Bucket=bucket_name,

        Key=parquet_key
    )

    parquet_data = BytesIO(
        response["Body"].read()
    )

    df = pd.read_parquet(
        parquet_data
    )

    return df


def write_parquet_to_s3(

    df,

    bucket_name,

    parquet_key
):

    parquet_buffer = BytesIO()

    datetime_columns = df.select_dtypes(
        include=["datetime"]
    ).columns

    for col in datetime_columns:

        df[col] = (
            df[col]
            .dt.floor("ms")
            .astype("datetime64[ms]")
        )

    df.to_parquet(

        parquet_buffer,

        index=False,

        engine="pyarrow"
    )

    s3_client.put_object(

        Bucket=bucket_name,

        Key=parquet_key,

        Body=parquet_buffer.getvalue()
    )

    print(
        "Remediated parquet "
        "uploaded to S3"
    )


def apply_safe_remediation(

    df,

    fixes
):

    remediation_log = []

    for fix in fixes:

        action = fix.get(
            "action"
        )

        column = fix.get(
            "column"
        )

        strategy = fix.get(
            "strategy"
        )

        if (
            column
            and
            column not in df.columns
        ):

            continue

        print(
            f"Applying fix: "
            f"{action}"
        )

        # -----------------------------------
        # NULL STRING FIX
        # -----------------------------------

        if action == "fill_nulls":

            before_nulls = (
                df[column]
                .isnull()
                .sum()
            )

            df[column] = (
                df[column]
                .fillna(strategy)
            )

            after_nulls = (
                df[column]
                .isnull()
                .sum()
            )

            remediation_log.append({

                "action": action,

                "column": column,

                "before_nulls": int(
                    before_nulls
                ),

                "after_nulls": int(
                    after_nulls
                )
            })

        # -----------------------------------
        # NUMERIC NULL FIX
        # -----------------------------------

        elif action == (
            "fill_numeric_nulls"
        ):

            before_nulls = (
                df[column]
                .isnull()
                .sum()
            )

            if strategy == "median":

                median_value = (
                    df[column]
                    .median()
                )

                df[column] = (
                    df[column]
                    .fillna(
                        median_value
                    )
                )

            elif strategy == "zero":

                df[column] = (
                    df[column]
                    .fillna(0)
                )

            after_nulls = (
                df[column]
                .isnull()
                .sum()
            )

            remediation_log.append({

                "action": action,

                "column": column,

                "before_nulls": int(
                    before_nulls
                ),

                "after_nulls": int(
                    after_nulls
                )
            })

        
        # -----------------------------------
        # WHITESPACE CLEANING
        # -----------------------------------

        elif action == (
            "strip_whitespace"
        ):

            df[column] = (

                df[column]
                .astype(str)
                .str.strip()
            )

            remediation_log.append({

                "action": action,

                "column": column
            })

        # -----------------------------------
        # INVALID DATE COERCION
        # -----------------------------------

        elif action == (
            "coerce_dates"
        ):

            invalid_before = (
                pd.to_datetime(

                    df[column],

                    errors="coerce"
                )
                .isnull()
                .sum()
            )

            df[column] = (
                pd.to_datetime(

                    df[column],

                    errors="coerce"
                )
            )

            invalid_after = (
                df[column]
                .isnull()
                .sum()
            )

            remediation_log.append({

                "action": action,

                "column": column,

                "invalid_before":
                int(invalid_before),

                "invalid_after":
                int(invalid_after)
            })

    return df, remediation_log


def remediate_dataset(

    bucket_name,

    parquet_key,

    llm_response
):

    fixes = llm_response.get(
        "recommended_fixes",
        []
    )

    if not fixes:

        print(
            "No remediation "
            "fixes found"
        )

        return {

            "remediation_applied":
            False,

            "log": []
        }

    df = read_parquet_from_s3(

        bucket_name,

        parquet_key
    )

    remediated_df, remediation_log = (

        apply_safe_remediation(

            df,

            fixes
        )
    )

    write_parquet_to_s3(

        remediated_df,

        bucket_name,

        parquet_key
    )

    print(
        "Safe remediation completed"
    )

    return {

        "remediation_applied":
        True,

        "log": remediation_log
    }
