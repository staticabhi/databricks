from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime, timedelta
import random

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create AE and LB DataFrames") \
    .getOrCreate()

# Helper function to generate random dates
def random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

# Constants for data generation
num_records = 100
study_id = "ST001"
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# Generate AE (Adverse Events) data
ae_data = []
subjects = ["100001", "100002", "100003", "100004", "100005", "100006", "100007", "100008", "100009", "100010"]
for ind in range(num_records):
    idx = ind%10
    subject_id = subjects[idx]
    # subject_id = f"{random.randint(100000, 999999)}"
    ae_term = random.choice(["Nausea", "Headache", "Abdominal Pain", "Dizziness", "Fatigue"])
    ae_start = random_date(start_date, end_date)
    ae_end = ae_start + timedelta(days=random.randint(1, 10))  # AE lasts 1 to 10 days
    ae_data.append((study_id, subject_id, ae_term, ae_start, ae_end))

# Define schema for AE
ae_schema = StructType([
    StructField("STUDYID", StringType(), True),
    StructField("USUBJID", StringType(), True),
    StructField("AETERM", StringType(), True),
    StructField("AESTDTC", DateType(), True),
    StructField("AEENDTC", DateType(), True),
])

# Create AE DataFrame
ae_df = spark.createDataFrame(ae_data, schema=ae_schema)
ae_df.show()

# Generate LB (Laboratory Results) data
lb_data = []
subjects = ["100001", "100002", "100003", "100004", "100005", "100006", "100007", "100008", "100009", "100010"]
for ind in range(num_records):
    idx = ind%10
    subject_id = subjects[idx]
    # subject_id = f"{random.randint(100000, 999999)}"
    lb_test = random.choice(["ALT", "AST", "Hemoglobin", "WBC", "Platelets"])
    lb_result = round(random.uniform(0.5, 150), 1)
    lb_flag = random.choice(["HIGH", "LOW", None])  # 1/3 chance of no abnormality
    lb_date = random_date(start_date, end_date)
    lb_data.append((study_id, subject_id, lb_test, lb_result, round(lb_result - random.uniform(5, 15), 1),
                    round(lb_result + random.uniform(5, 15), 1), lb_flag, lb_date))

# Define schema for LB
lb_schema = StructType([
    StructField("STUDYID", StringType(), True),
    StructField("USUBJID", StringType(), True),
    StructField("LBTEST", StringType(), True),
    StructField("LBORRES", DoubleType(), True),
    StructField("LBSTNRLO", DoubleType(), True),
    StructField("LBSTNRHI", DoubleType(), True),
    StructField("LBSTRESC", StringType(), True),
    StructField("LBDTC", DateType(), True),
])

# Create LB DataFrame
lb_df = spark.createDataFrame(lb_data, schema=lb_schema)
lb_df.show()

# Print schemas to verify
print("AE Schema:")
ae_df.printSchema()

print("LB Schema:")
lb_df.printSchema()

# Set the database/schema name
database_name = "clinical_trials"

# Create the database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Set the current database
spark.sql(f"USE {database_name}")

# Write AE DataFrame to the schema
ae_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ae_data")

# Write LB DataFrame to the schema
lb_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.lb_data")

# Verify the tables have been created
spark.sql("SHOW TABLES").show()

