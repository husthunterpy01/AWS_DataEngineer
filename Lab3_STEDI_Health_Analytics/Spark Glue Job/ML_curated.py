import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Get the job name from the arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize the Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load Accelerometer Trusted data from S3
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://project-lakehouse-bucket3/accelerometer/trusted/"], "recurse": True},
    format_options={"multiline": False},
    transformation_ctx="AccelerometerTrusted_node"
)

# Load Step Trainer Trusted data from S3
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://project-lakehouse-bucket3/step_trainer/trusted/"], "recurse": True},
    format_options={"multiline": False},
    transformation_ctx="StepTrainerTrusted_node"
)

# Convert the DynamicFrames to DataFrames for SQL queries
accelerometer_trusted_df = AccelerometerTrusted_node.toDF()
step_trainer_trusted_df = StepTrainerTrusted_node.toDF()

# Register the DataFrames as temporary views for SQL queries
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")
step_trainer_trusted_df.createOrReplaceTempView("step_trainer_trusted")

# Perform SQL join to match the timestamp from both tables
joined_df = spark.sql("""
    SELECT *
    FROM accelerometer_trusted a
    JOIN step_trainer_trusted s
    ON a.timestamp = s.sensorreadingtime
""")

# Remove duplicates if necessary
joined_df = joined_df.dropDuplicates()

# Convert the joined DataFrame back to a DynamicFrame for Glue
final_dynamic_frame = DynamicFrame.fromDF(joined_df, glueContext, "final_dynamic_frame")

# Write the resulting DynamicFrame to S3 in JSON format
output_path = "s3://project-lakehouse-bucket3/accelerometer/curated/"
glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": output_path,
        "partitionKeys": [],
        "enableUpdateCatalog": True
    },
    transformation_ctx="CustomerCurated_node"
)

# Commit the job to finish
job.commit()
