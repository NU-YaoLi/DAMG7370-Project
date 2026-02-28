import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, to_date, current_timestamp, year, coalesce

def to_lower_case(name):
    # Adds underscore before uppercase letters and lowercases everything
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DB_ENDPOINT', 'DB_PASSWORD'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DB_ENDPOINT = args['DB_ENDPOINT']
DB_PASSWORD = args['DB_PASSWORD']
BUCKET_NAME = "damg7370-house-investment-data-lake"

df_raw = spark.read.csv(f"s3://{BUCKET_NAME}/raw/*total_monthly_payment*.csv", header=True, inferSchema=True)

id_cols = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]
date_cols = [c for c in df_raw.columns if c not in id_cols]

stack_expr = f"stack({len(date_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in date_cols]) + ") as (date_str, monthly_payment)"

df_transformed = (
    df_raw.select(*id_cols, expr(stack_expr))
    .withColumn("metric_date", coalesce(to_date(col("date_str"), "M/d/yyyy"), to_date(col("date_str"), "yyyy-MM-dd")))
    .drop("date_str")
    .dropna(subset=["monthly_payment"])
    .withColumn("year", year(col("metric_date")))
    .withColumn("inserted_at", current_timestamp())
)

for c in df_transformed.columns:
    df_transformed = df_transformed.withColumnRenamed(c, to_lower_case(c))

# (df_transformed.write.mode("overwrite").partitionBy("StateName", "year")
#  .parquet(f"s3://{BUCKET_NAME}/processed/monthly_payments/"))

(df_transformed.write.format("jdbc")
 .option("url", f"jdbc:postgresql://{DB_ENDPOINT}:5432/postgres")
 .option("dbtable", "stg_monthly_payment") # UPDATED to staging prefix
 .option("user", "dbadmin").option("password", DB_PASSWORD)
 .option("driver", "org.postgresql.Driver").mode("overwrite").save())

job.commit()