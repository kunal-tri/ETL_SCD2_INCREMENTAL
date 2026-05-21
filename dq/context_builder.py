import json
import boto3


s3_client = boto3.client("s3")


def build_llm_context(profile):

    context = {}

    dataset_summary = {

        "dataset_name": (
            profile["dataset_name"]
        ),

        "profile_timestamp": (
            profile["profile_timestamp"]
        ),

        "row_count": (
            profile["row_count"]
        ),

        "column_count": (
            profile["column_count"]
        ),

        "duplicate_count": (
            profile["duplicate_count"]
        ),

        "memory_usage_mb": (
            profile["memory_usage_mb"]
        )
    }

    context["dataset_summary"] = (
        dataset_summary
    )

    context["health_flags"] = {

        "has_nulls": (
            profile["health_flags"][
                "has_nulls"
            ]
        ),

        "has_duplicates": (
            profile["health_flags"][
                "has_duplicates"
            ]
        ),

        "has_empty_columns": (
            profile["health_flags"][
                "has_empty_columns"
            ]
        ),

        "is_empty_dataset": (
            profile["health_flags"][
                "is_empty_dataset"
            ]
        )
    }

    high_null_columns = {}

    for column, percentage in (
        profile[
            "null_percentages"
        ].items()
    ):

        if percentage > 0:

            high_null_columns[column] = (
                percentage
            )

    context["null_analysis"] = {

        "columns_with_nulls": (
            high_null_columns
        ),

        "total_columns_with_nulls": (
            len(high_null_columns)
        )
    }

    schema_analysis = {

        "schema": (
            profile["schema"]
        ),

        "total_columns": (
            len(profile["schema"])
        ),

        "empty_columns": (
            profile["empty_columns"]
        ),

        "fully_unique_columns": (
            profile[
                "fully_unique_columns"
            ]
        )
    }

    context["schema_analysis"] = (
        schema_analysis
    )

    numeric_analysis = {}

    for column, stats in (
        profile[
            "numeric_stats"
        ].items()
    ):

        numeric_analysis[column] = {

            "mean": stats["mean"],

            "std": stats["std"],

            "min": stats["min"],

            "max": stats["max"]
        }

    context["numeric_analysis"] = (
        numeric_analysis
    )

    distinct_analysis = {}

    for column, distinct_count in (
        profile[
            "distinct_counts"
        ].items()
    ):

        distinct_analysis[column] = {

            "distinct_count": (
                distinct_count
            ),

            "total_rows": (
                profile["row_count"]
            )
        }

    context["distinct_analysis"] = (
        distinct_analysis
    )

    context["sample_records"] = (
        profile["sample_records"]
    )

    context["llm_instruction"] = """

You are an AI Data Quality Analyst.

Analyze the dataset metrics carefully.

Tasks:
1. Detect anomalies
2. Detect suspicious patterns
3. Detect schema problems
4. Detect abnormal null percentages
5. Detect duplicate issues
6. Classify severity
7. Suggest root causes
8. Recommend remediation steps

Return response in JSON format.

"""

    return context


def context_to_json(context):

    return json.dumps(
        context,
        indent=4
    )


def save_context_to_s3(
    context,
    bucket_name,
    object_key
):

    json_data = json.dumps(
        context,
        indent=4
    )

    s3_client.put_object(

        Bucket=bucket_name,

        Key=object_key,

        Body=json_data,

        ContentType="application/json"
    )

    print(
        f"Context saved to "
        f"s3://{bucket_name}/"
        f"{object_key}"
    )


def load_context_from_s3(
    bucket_name,
    object_key
):

    response = s3_client.get_object(

        Bucket=bucket_name,

        Key=object_key
    )

    context_data = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    context = json.loads(
        context_data
    )

    print(
        f"Context loaded from "
        f"s3://{bucket_name}/"
        f"{object_key}"
    )

    return context
