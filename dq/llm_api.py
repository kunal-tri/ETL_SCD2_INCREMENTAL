import json
import boto3
import traceback
import streamlit as st

from groq import Groq


GROQ_API_KEY = st.secrets[
    "GROQ_API_KEY"
]


client = Groq(
    api_key=GROQ_API_KEY
)


MODEL_NAME = (
    "llama-3.1-8b-instant"
)


s3_client = boto3.client(
    "s3"
)


def load_context_from_s3(
    bucket_name,
    object_key
):

    try:

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
            f"Loaded context from "
            f"s3://{bucket_name}/"
            f"{object_key}"
        )

        return context

    except Exception as e:

        print(
            f"Failed to load context: "
            f"{e}"
        )

        return None


def save_json_to_s3(
    data,
    bucket_name,
    object_key
):

    try:

        json_data = json.dumps(
            data,
            indent=4
        )

        s3_client.put_object(

            Bucket=bucket_name,

            Key=object_key,

            Body=json_data,

            ContentType=(
                "application/json"
            )
        )

        print(
            f"Saved to "
            f"s3://{bucket_name}/"
            f"{object_key}"
        )

    except Exception as e:

        print(
            f"Failed to save JSON: "
            f"{e}"
        )


def build_prompt(context):

    prompt = f"""

You are an AI-powered Data Quality Analyst.

Analyze the dataset carefully.

Responsibilities:

1. Detect anomalies
2. Detect abnormal row counts
3. Detect high null percentages
4. Detect duplicate issues
5. Detect schema inconsistencies
6. Detect suspicious patterns
7. Detect abnormal numeric distributions
8. Classify severity
9. Suggest root causes
10. Recommend remediation
11. Summarize dataset health

Allowed Safe Remediation Actions:

1. fill_nulls
2. fill_numeric_nulls
3. strip_whitespace
4. coerce_dates

Rules:

- Return ONLY valid JSON
- No markdown
- No explanations outside JSON
- Only use allowed remediation actions
- Never suggest deleting datasets
- Never suggest dangerous transformations

Dataset Context:

{json.dumps(context, indent=4)}

Expected JSON Format:

{{
    "anomaly_detected": true,

    "severity": "HIGH",

    "issues": [],

    "root_cause": "",

    "recommendation": "",

    "summary": "",

    "recommended_fixes": [

        {{
            "action": "",

            "column": "",

            "strategy": ""
        }}
    ]
}}

"""

    return prompt


def call_llm(prompt):

    try:

        completion = (

            client.chat.completions.create(

                model=MODEL_NAME,

                messages=[

                    {
                        "role": "user",

                        "content": prompt
                    }
                ],

                temperature=0
            )
        )

        result = (
            completion
            .choices[0]
            .message.content
        )

        return result

    except Exception as e:

        raise Exception(
            f"Groq API failed: {e}"
        )


def parse_llm_response(response):

    try:

        cleaned_response = (

            response
            .replace("```json", "")
            .replace("```", "")
            .strip()
        )

        parsed_response = json.loads(
            cleaned_response
        )

        if (
            "recommended_fixes"
            not in parsed_response
        ):

            parsed_response[
                "recommended_fixes"
            ] = []

        return parsed_response

    except Exception as e:

        return {

            "anomaly_detected": False,

            "severity": "ERROR",

            "issues": [
                "JSON parsing failed"
            ],

            "root_cause": str(e),

            "recommendation": (
                "Check Groq response "
                "format"
            ),

            "summary": response,

            "recommended_fixes": []
        }


def analyze_dataset_from_s3(

    bucket_name,

    context_object_key,

    response_object_key
):

    try:

        context = load_context_from_s3(

            bucket_name,

            context_object_key
        )

        if not context:

            raise Exception(
                "Context loading failed"
            )

        prompt = build_prompt(
            context
        )

        llm_response = call_llm(
            prompt
        )

        parsed_response = (
            parse_llm_response(
                llm_response
            )
        )

        save_json_to_s3(

            parsed_response,

            bucket_name,

            response_object_key
        )

        return parsed_response

    except Exception as e:

        error_response = {

            "anomaly_detected": False,

            "severity": "ERROR",

            "issues": [
                str(e)
            ],

            "root_cause": (
                traceback.format_exc()
            ),

            "recommendation": (
                "Check Groq API, "
                "S3 access, "
                "or prompt formatting"
            ),

            "summary": (
                "LLM analysis failed"
            ),

            "recommended_fixes": []
        }

        save_json_to_s3(

            error_response,

            bucket_name,

            response_object_key
        )

        return error_response
