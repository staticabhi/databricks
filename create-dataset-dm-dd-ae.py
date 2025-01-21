from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Domain Datasets") \
    .getOrCreate()

# Helper function to generate random dates
def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# Generate AE Dataset
ae_data = [(f"SUBJ_{i:03d}", f"SITE_{random.randint(1, 5)}", f"AE_LOG_{i:03d}",
            f"AETERM_{random.randint(1, 10)}", f"AEDECODE_{random.randint(1, 10)}",
            random_date(datetime(2023, 1, 1), datetime(2023, 6, 1)),
            random_date(datetime(2023, 6, 2), datetime(2023, 12, 31)))
           for i in range(100)]
ae_columns = ["usubjid", "siteid", "AELogline", "AETERM", "AEDECODE", "AESTARTDATE", "AEENDDATE"]
ae_df = spark.createDataFrame(ae_data, schema=ae_columns)

# Generate DD Dataset
dd_data = [(f"SUBJ_{i:03d}", f"SITE_{random.randint(1, 5)}",
            random_date(datetime(2024, 1, 1), datetime(2024, 12, 31)))
           for i in range(100)]
dd_columns = ["usubjid", "siteid", "DDDATE"]
dd_df = spark.createDataFrame(dd_data, schema=dd_columns)

# Generate DM Dataset
dm_data = [(f"SUBJ_{i:03d}", f"SITE_{random.randint(1, 5)}",
            random_date(datetime(2022, 1, 1), datetime(2022, 12, 31)))
           for i in range(100)]
dm_columns = ["usubjid", "siteid", "RFICDATE"]
dm_df = spark.createDataFrame(dm_data, schema=dm_columns)

# Show the datasets
print("AE Dataset:")
ae_df.show(truncate=False)

print("DD Dataset:")
dd_df.show(truncate=False)

print("DM Dataset:")
dm_df.show(truncate=False)


ae_df.write.mode("overwrite").saveAsTable("basic_ae")
dd_df.write.mode("overwrite").saveAsTable("basic_dd")
dm_df.write.mode("overwrite").saveAsTable("basic_dm")

spark.sql("SHOW TABLES").show()
