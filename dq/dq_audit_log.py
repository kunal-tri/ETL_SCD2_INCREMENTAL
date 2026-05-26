import json
import boto3

from datetime import datetime
from zoneinfo import ZoneInfo


INDIA_TZ = ZoneInfo("Asia/Kolkata")


s3_client = boto3.client(
    "s3"
)


AUDIT_PREFIX = (
    "dq-audit/"
)


def save_audit_log(

    bucket_name,

    dataset_name,

    llm_response,

    remediation_result
):

    timestamp = datetime.now(
        INDIA_TZ
    )

    timestamp_str = (
        timestamp.strftime(
            "%Y%m%d_%H%M%S"
        )
    )

    audit_data = {

        "dataset_name":dataset_name,

        "audit_timestamp":str(timestamp),

        "severity":llm_response.get("severity"),

        "issues":llm_response.get("issues",[]),

        "recommended_fixes":llm_response.get("recommended_fixes",[]),

        "remediation_applied":remediation_result.get("remediation_applied",False),

        "remediation_log":remediation_result.get("log",[]),

        "summary":llm_response.get("summary"),

        "recommendation":llm_response.get("recommendation")
    }

    object_key = (

        f"{AUDIT_PREFIX}"
        f"{dataset_name}/"
        f"audit_"
        f"{timestamp_str}.json"
    )

    s3_client.put_object(

        Bucket=bucket_name,

        Key=object_key,

        Body=json.dumps(
            audit_data,
            indent=4
        ),

        ContentType=("application/json")
    )

    print(
        f"Audit log saved: "
        f"s3://{bucket_name}/"
        f"{object_key}"
    )

    return object_key
