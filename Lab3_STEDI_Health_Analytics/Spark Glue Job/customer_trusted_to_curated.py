import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from Glue catalog into DynamicFrames
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="steadi1",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_dyf"
)

step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="steadi1",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_dyf"
)

# Convert DynamicFrames to DataFrames for Spark SQL
accelerometer_trusted_df = accelerometer_trusted_dyf.toDF()
step_trainer_trusted_df = step_trainer_trusted_dyf.toDF()

# Register the DataFrames as temporary views
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")
step_trainer_trusted_df.createOrReplaceTempView("step_trainer_trusted")

# Perform the SQL join operation
joined_df = spark.sql("""
SELECT *
FROM accelerometer_trusted a
JOIN step_trainer_trusted s
ON a.timestamp = s.sensorreadingtime
""")

# Remove duplicates if necessary
joined_df = joined_df.dropDuplicates()

# Coalesce to a single partition for a single output file
output_df = joined_df.coalesce(1)

# Write the result as a single JSON file
output_path = "s3://project-lakehouse-bucket3/accelerometer/curated/"
output_df.write.mode("overwrite").json(output_path)

job.commit()
