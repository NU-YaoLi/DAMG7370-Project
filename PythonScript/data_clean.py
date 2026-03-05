import pandas as pd
import numpy as np
import sys
import psycopg2
from awsglue.utils import getResolvedOptions

# --- DATABASE SETUP ---
# Fetch DB credentials from Glue Job parameters
args = getResolvedOptions(sys.argv, ['DB_ENDPOINT', 'DB_PASSWORD'])
DB_ENDPOINT = args['DB_ENDPOINT']
DB_PASSWORD = args['DB_PASSWORD']
DB_USER = "dbadmin"
DB_NAME = "postgres"

def create_landing_schema():
    print("Connecting to database to ensure 'landing' schema exists...")
    try:
        # Establish connection to RDS
        conn = psycopg2.connect(
            host=DB_ENDPOINT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # DDL commands like CREATE SCHEMA need autocommit enabled
        conn.autocommit = True 
        cursor = conn.cursor()
        
        # Execute the SQL command
        cursor.execute("CREATE SCHEMA IF NOT EXISTS landing;")
        print("Schema 'landing' verified/created successfully.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to create schema: {e}")
        raise e # Fail the job if it can't reach the database

# --- S3 DATA PROCESSING ---
def clean_and_fill_s3(fill_ln_num, input_path, output_path):
    print(f"Reading from: {input_path}")
    
    # Pandas can read directly from S3 using the s3:// prefix
    df = pd.read_csv(input_path)

    # 1. Trim whitespace
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 2. Identify date columns
    date_cols = df.columns[fill_ln_num:] 

    # 3. Interpolate horizontally
    df[date_cols] = df[date_cols].interpolate(
        method='linear', 
        axis=1, 
        limit_area='inside'
    )

    # 4. Write back to the 'cleaned' bucket
    df.to_csv(output_path, index=False)
    print(f"Successfully saved to: {output_path}")

# --- CONFIGURATION ---
S3_RAW = "s3://damg7370-house-investment-data-lake/raw/"
S3_CLEANED = "s3://damg7370-house-investment-data-lake/cleaned/"

# List of your files and their specific fill line numbers
files_to_process = [
    (8, "raw_City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv", "cleaned_City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv"),
    (8, "raw_City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv", "cleaned_City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    (8, "raw_City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv", "cleaned_City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv"),
    (5, "raw_Metro_total_monthly_payment_downpayment_0.20_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv", "cleaned_Metro_total_monthly_payment_downpayment.csv"),
    (9, "raw_Zip_zori_uc_sfrcondomfr_sm_sa_month.csv", "cleaned_Zip_zori.csv")
]

# 1. First, ensure the schema exists in the database
create_landing_schema()

# 2. Then, process the S3 files
for fill_num, in_file, out_file in files_to_process:
    clean_and_fill_s3(fill_num, S3_RAW + in_file, S3_CLEANED + out_file)