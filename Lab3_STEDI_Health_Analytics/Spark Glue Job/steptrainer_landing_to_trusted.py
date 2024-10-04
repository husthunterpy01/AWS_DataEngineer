import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Add this import for DynamicFrame

# Get job name from the arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize the Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load Step Trainer Landing data from S3
StepTrainerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lakehouse-bucket3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node",
)

# Load Customer Curated data from S3
CustomersCurated_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lakehouse-bucket3/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node",
)

# Convert the DynamicFrames to DataFrames for SQL queries
step_trainer_landing_df = StepTrainerLanding_node.toDF()
customers_curated_df = CustomersCurated_node.toDF()

# Register the DataFrames as temporary views for SQL queries
step_trainer_landing_df.createOrReplaceTempView("step_trainer_landing")
customers_curated_df.createOrReplaceTempView("customers_curated")

# Perform SQL join to match serial numbers and get only step trainer landing data
result_df = spark.sql("""
    SELECT s.*
    FROM step_trainer_landing s
    INNER JOIN customers_curated c
    ON s.serialNumber = c.serialNumber
""")

# Convert the final result DataFrame back to a DynamicFrame
final_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "final_dynamic_frame")

# Write the resulting DynamicFrame to S3 in JSON format
CustomerCurated_node = glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-lakehouse-bucket3/step_trainer/trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True
    },
    transformation_ctx="CustomerCurated_node",
)

# Commit the job to finish
job.commit()
