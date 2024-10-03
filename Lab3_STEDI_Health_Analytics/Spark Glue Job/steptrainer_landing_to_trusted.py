from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark SQL Conversion") \
    .getOrCreate()

# Load data into DataFrames
step_trainer_landing_df = spark.read.json("s3://project-lakehouse-bucket3/step_trainer/landing/")
customers_curated_df = spark.read.json("s3://project-lakehouse-bucket3/customer/curated/")

# Register the DataFrames as temporary views for SQL queries
step_trainer_landing_df.createOrReplaceTempView("step_trainer_landing")
customers_curated_df.createOrReplaceTempView("customers_curated")

# Execute the SQL query, selecting only columns from step_trainer_landing
result_df = spark.sql("""
    SELECT 
        s.*
    FROM step_trainer_landing s
    INNER JOIN customers_curated c
    ON s.serialNumber = c.serialNumber
""")

# Coalesce to a single partition and save as one JSON file
result_df.coalesce(1) \
    .write.mode("overwrite") \
    .json("s3://project-lakehouse-bucket3/step_trainer/trusted/")

# Stop the Spark session
spark.stop()
