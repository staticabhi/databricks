from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schema
schema = StructType([ StructField("subject_id", StringType(), True), StructField("visit_id", IntegerType(), True), StructField("vstest", StringType(), True), StructField("vsorres", DoubleType(), True), StructField("visit_date", DateType(), True) ])


from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random
# Initialize SparkSession
spark = SparkSession.builder.appName("Vital Signs DataFrame").getOrCreate()

# Generate sample data
def generate_data(num_records):
    data = []
    subjects = ["10000" + str(x) for x in range(1,11)]
    for ind in range(num_records):
        idx = ind%10
        # subject_id = f"{random.randint(100000, 999999)}" 
        subject_id = subjects[idx]
        # 6-digit subject ID
        visit_id = random.randint(1, 5)
        vstest = random.choice(["BP", "HR", "TEMP", "RR"])
        vsorres = round(random.uniform(60, 180), 1) if random.random() > 0.1 else None
        visit_date = (datetime.now() - timedelta(days=random.randint(1, 365))).date() \
             if random.random() > 0.1 else None
        data.append((subject_id, visit_id, vstest, vsorres, visit_date))
    return data

data = generate_data(100)
vital_signs_df = spark.createDataFrame(data, schema=schema)


vital_signs_df.display()


vital_signs_df.write.mode("overwrite").saveAsTable("vital_signs_table")
spark.sql("SHOW TABLES").show()



