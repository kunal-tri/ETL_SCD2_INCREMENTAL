import pandas as pd
from io import StringIO

from alerts import (
    send_slack_alert,
    send_email_alert
)


def merge_csv_with_s3(
    s3_client,
    bucket_name,
    s3_key,
    uploaded_file,
    date_cols
):

    uploaded_file.seek(0)

    # READ NEW CSV
    new_df = pd.read_csv(
        uploaded_file,
        dtype=str
    )

    # CLEAN NEW DATA
    new_df.columns = (
        new_df.columns
        .str.strip()
    )

    for col in new_df.columns:

        new_df[col] = (
            new_df[col]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    # DATE PARSING
    for col in date_cols:

        if col in new_df.columns:

            new_df[col] = pd.to_datetime(
                new_df[col],
                errors="coerce"
            )

    # LOAD EXISTING CSV
    try:

        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=s3_key
        )

        existing_csv = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        existing_df = pd.read_csv(
            StringIO(existing_csv),
            dtype=str
        )

        existing_df.columns = (
            existing_df.columns
            .str.strip()
        )

        for col in existing_df.columns:

            existing_df[col] = (
                existing_df[col]
                .fillna("")
                .astype(str)
                .str.strip()
            )

        # DATE PARSING
        for col in date_cols:

            if col in existing_df.columns:

                existing_df[col] = (
                    pd.to_datetime(
                        existing_df[col],
                        errors="coerce"
                    )
                )

    except Exception:

        existing_df = pd.DataFrame()

    # REMOVE SCD2 COLUMNS
    scd_columns = [
        "start_date",
        "end_date",
        "is_current",
        "etl_timestamp"
    ]

    existing_df = existing_df.drop(
        columns=[
            col for col in scd_columns
            if col in existing_df.columns
        ],
        errors="ignore"
    )

    new_df = new_df.drop(
        columns=[
            col for col in scd_columns
            if col in new_df.columns
        ],
        errors="ignore"
    )

    # COLUMN VALIDATION
    if not existing_df.empty:

        existing_cols = sorted(
            existing_df.columns.tolist()
        )

        new_cols = sorted(
            new_df.columns.tolist()
        )

        if existing_cols != new_cols:

            error_message = (
                "CSV Merge Failed ❌\n"
                "Wrong CSV Uploaded.\n"
                "Column structure mismatch detected."
            )

            send_slack_alert(error_message)

            send_email_alert(
                subject="CSV Merge Failed",
                body=error_message
            )

            raise Exception(
                "Column mismatch between "
                "existing CSV and uploaded CSV"
            )

    # HASH COMPARISON
    if existing_df.empty:

        incremental_df = new_df.copy()

    else:

        existing_row_hash = (
            existing_df
            .fillna("")
            .astype(str)
            .apply(
                lambda row:
                "|".join(map(str, row.values)),
                axis=1
            )
        )

        new_row_hash = (
            new_df
            .fillna("")
            .astype(str)
            .apply(
                lambda row:
                "|".join(map(str, row.values)),
                axis=1
            )
        )

        incremental_df = new_df[
            ~new_row_hash.isin(
                set(existing_row_hash)
            )
        ]

    # MERGE
    combined_df = pd.concat(
        [
            existing_df,
            incremental_df
        ],
        ignore_index=True
    )

    # SAVE TO S3
    csv_buffer = StringIO()

    save_df = combined_df.copy()

    for col in date_cols:

        if col in save_df.columns:

            save_df[col] = (
                save_df[col]
                .astype(str)
            )

    save_df.to_csv(
        csv_buffer,
        index=False
    )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )

    return {
        "existing_df": existing_df,
        "new_df": new_df,
        "incremental_df": incremental_df,
        "combined_df": combined_df
    }
