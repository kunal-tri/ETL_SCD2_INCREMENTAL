import boto3
import streamlit as st
import pandas as pd

from io import StringIO
from merge_csv import merge_csv_with_s3
from incre import run_incremental_scd2_pipeline

# PAGE CONFIG

st.set_page_config(
    page_title="NBFC Incremental Pipeline",
    page_icon="📊",
    layout="wide"
)

st.title(
    "📊 NBFC Incremental "
    "SCD2 Pipeline"
)


# AWS CONFIG

AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]

AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]

AWS_REGION = st.secrets["AWS_REGION"]

BUCKET_NAME = st.secrets["S3_BUCKET"]

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)


# TABLES

TABLES = [
    "customer",
    "loan",
    "document"
]


# DATE COLUMNS

DATE_COLUMNS = {

    "customer": [],

    "loan": [

        "Start_Date",

        "Disbursed_Date"
    ],

    "document": []
}


# SELECT TABLE

selected_table = st.selectbox(
    "Select Dataset",
    TABLES
)


# PIPELINE MODE

pipeline_mode = st.radio(

    "Choose Pipeline Type",

    [
        "Incremental SCD2 Pipeline",
        "Full ETL Pipeline"
    ]
)

st.markdown("---")

# INPUT MODE

input_mode = st.radio(
    "Choose Input Method",
    [
        "Upload CSV",
        "Manual Entry"
    ]
)


# CSV UPLOAD MODE

if input_mode == "Upload CSV":

    uploaded_file = st.file_uploader(
        "Upload CSV File",
        type=["csv"]
    )

    if uploaded_file is not None:

        try:

            df = pd.read_csv(uploaded_file,dtype=str)

        except Exception as e:

            st.error(f"CSV Read Error: {e}")

            st.stop()

        st.success("CSV Uploaded Successfully")

        st.subheader("Dataset Preview")

        st.dataframe(
            df.head(20),
            width='stretch'
        )

        st.write(f"Rows: {len(df)}")

        st.write(f"Columns: {len(df.columns)}")

        st.markdown("---")

        if st.button("🚀 Run Pipeline"):

            with st.spinner("Running Incremental "  "Pipeline"):

                try:

                    st.info(
                        "STEP 1 — "
                        "Merging CSV "
                        "with Existing "
                        "S3 Data"
                    )

                    # S3 FILE PATHS

                    if selected_table == "customer":

                        s3_key = (
                            "folder/"
                            "nbfc_customer_data.csv"
                        )

                    elif selected_table == "loan":

                        s3_key = (
                            "folder/"
                            "nbfc_loan_data.csv"
                        )

                    else:

                        s3_key = (
                            "folder/"
                            "nbfc_document_data.csv"
                        )

                    merge_result = merge_csv_with_s3(
                        s3_client=s3_client,
                        bucket_name=BUCKET_NAME,
                        s3_key=s3_key,
                        uploaded_file=uploaded_file,
                        date_cols=date_cols
                    )
                    
                    existing_df = merge_result["existing_df"]
                    
                    new_df = merge_result["new_df"]
                    
                    incremental_df = merge_result["incremental_df"]
                    
                    combined_df = merge_result["combined_df"]
                    
                    st.success(
                        "CSV merged "
                        "successfully "
                        "with S3 data"
                    )

                    st.write(
                        f"Existing Rows "
                        f"in S3: "
                        f"{len(existing_df)}"
                    )

                    st.write(f"Uploaded Rows: "f"{len(new_df)}")

                    st.write(f"True Incremental Rows: "f"{len(incremental_df)}")

                    st.write(f"Duplicate Rows " f"Removed: " f"{duplicates_removed}")

                    st.write(
                        f"Total Rows "
                        f"After Merge: "
                        f"{len(combined_df)}"
                    )

                    # PIPELINE MODE

                    if (pipeline_mode=="Incremental SCD2 Pipeline"):

                        st.info(
                            "STEP 2 — "
                            "Running "
                            "Incremental SCD2"
                        )

                        process_df = incremental_df

                    else:

                        st.info(
                            "STEP 2 — "
                            "Running "
                            "Full ETL Pipeline"
                        )

                        process_df = combined_df

                    # RUN PIPELINE
                    result = (
                        run_incremental_scd2_pipeline(
                            source_df=process_df,
                            table_name=selected_table
                        )
                    )

                    final_df = result["final_df"]

                    summary = result["monitoring_summary"]

                    st.success(
                        "Incremental "
                        "SCD2 Completed"
                    )

                    st.subheader(
                        "Final Incremental "
                        "Dataset"
                    )

                    st.dataframe(
                        final_df.tail(50),
                        width='stretch'
                    )

                    col1, col2, col3 = (st.columns(3))

                    col1.metric("Total Rows",len(final_df))

                    if ("is_current"in final_df.columns):

                        active_count = (final_df["is_current"]== "Y").sum()

                        historical_count = (final_df["is_current"]== "N").sum()

                        col2.metric("Active Records",active_count)

                        col3.metric("Historical Records",historical_count)

                    st.subheader("Pipeline Monitoring ""Summary")

                    st.json(summary)

                    # DOWNLOAD BUTTON

                    csv_download = (
                        final_df.to_csv(
                            index=False
                        )
                    )

                    st.download_button(

                        label="⬇ Download Final Dataset",

                        data=csv_download,

                        file_name=(f"{selected_table}_final_output.csv"),

                        mime="text/csv"
                    )

                    st.balloons()

                except Exception as e:

                    st.error(
                        f"Pipeline Failed: "
                        f"{e}"
                    )


# MANUAL ENTRY MODE

elif input_mode == "Manual Entry":

    st.subheader("Manual Data Entry")

    # CUSTOMER

    if selected_table == "customer":

        with st.form("customer_form"):

            cust_id = st.text_input("Customer ID")

            phone = st.text_input("Phone")

            first_name = st.text_input("First Name")

            last_name = st.text_input("Last Name")

            address = st.text_area("Address")

            pincode = st.text_input("Pincode")

            submit_customer = (
                st.form_submit_button(
                    "Submit Customer Record"
                )
            )

            if submit_customer:

                customer_df = pd.DataFrame([
                    {
                        "cust_id": cust_id,
                        "Phone": phone,
                        "First Name": first_name,
                        "Last Name": last_name,
                        "Address": address,
                        "Pincode": pincode
                    }
                ])

                try:

                    result = (
                        run_incremental_scd2_pipeline(
                            source_df=customer_df,
                            table_name="customer"
                        )
                    )

                    final_df = result["final_df"]

                    st.success("Customer Record Processed")
                    display_df = final_df
                    st.dataframe(
                        display_df.tail(20),
                        width='stretch'
                    )

                except Exception as e:

                    st.error(f"Pipeline Failed: {e}")

    # LOAN

    elif selected_table == "loan":

        with st.form("loan_form"):

            cust_id = st.text_input("Customer ID")

            loan_id = st.text_input("Loan ID")

            sanction_loan = (st.number_input("Sanction Loan"))

            cibil_score = (st.number_input("CIBIL Score"))

            start_date = st.date_input("Start Date")

            disbursed_date = st.date_input("Disbursed Date")

            location = st.text_input("Location")

            pincode = st.text_input("Pincode")

            loan_type = st.text_input("Loan Type")

            unit = st.text_input("Unit")

            submit_loan = (st.form_submit_button("Submit Loan Record"))

            if submit_loan:

                loan_df = pd.DataFrame([
                    {
                        "cust_id": cust_id,
                        "loan_id": loan_id,
                        "Sanction_Loan": sanction_loan,
                        "Cibil_Score": cibil_score,
                        "Start_Date": pd.to_datetime(start_date),
                        "Disbursed_Date": pd.to_datetime(disbursed_date),
                        "Location": location,
                        "Pincode": pincode,
                        "Loan_Type": loan_type,
                        "Unit": unit
                    }
                ])

                try:

                    result = (
                        run_incremental_scd2_pipeline(
                            source_df=loan_df,
                            table_name="loan"
                        )
                    )

                    final_df = result["final_df"]

                    st.success("Loan Record Processed")
                    display_df = final_df
                    st.dataframe(
                        display_df.tail(20),
                        width='stretch'
                    )
                except Exception as e:
                    st.error(f"Pipeline Failed: {e}")

    # DOCUMENT

    elif selected_table == "document":

        with st.form("document_form"):

            cust_id = st.text_input("Customer ID")

            kyc_status = st.selectbox("KYC Status",["Yes", "No"])

            annual_income = (st.number_input("Annual Income"))

            nationality = st.text_input("Nationality")

            application_type = st.text_input("Application Type")

            submit_document = (st.form_submit_button("Submit Document Record"))

            if submit_document:

                document_df = pd.DataFrame([
                    {
                        "cust_id": cust_id,
                        "KYC_Status": kyc_status,
                        "Annual_Income": annual_income,
                        "Nationality": nationality,
                        "Application_Type": application_type
                    }
                ])

                try:

                    result = (
                        run_incremental_scd2_pipeline(
                            source_df=document_df,
                            table_name="document"
                        )
                    )

                    final_df = result["final_df"]

                    st.success("Document Record Processed")

                    display_df = final_df
                    st.dataframe(
                        display_df.tail(20),
                        width='stretch'
                    )

                except Exception as e:

                    st.error(f"Pipeline Failed: {e}")
