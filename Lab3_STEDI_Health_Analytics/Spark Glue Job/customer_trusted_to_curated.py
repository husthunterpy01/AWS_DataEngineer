import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load Customer Trusted data from S3
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lakehouse-bucket3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Load Accelerometer Trusted data from S3
AccelerometerTrusted_node1690329778730 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lakehouse-bucket3/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1690329778730",
)

# Convert DynamicFrames to DataFrames for SQL operations
customer_trusted_df = CustomerTrusted_node1.toDF()
accelerometer_trusted_df = AccelerometerTrusted_node1690329778730.toDF()

# Register the DataFrames as temporary views
customer_trusted_df.createOrReplaceTempView("customer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")

# Perform the SQL join operation
joined_df = spark.sql("""
    SELECT c.*, a.*
    FROM customer_trusted c
    JOIN accelerometer_trusted a
    ON c.email = a.user
""")

# Drop duplicates after the join operation based on columns that uniquely identify a record
# Adjust the columns used in the dropDuplicates method if necessary
joined_df = joined_df.dropDuplicates(subset=["email", "serialnumber", "registrationdate"])

# Drop specific columns that are not needed
final_df = joined_df.drop("timeStamp", "z", "user", "y", "x")

# Convert the final DataFrame back to DynamicFrame
final_dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "final_dynamic_frame")

# Script generated for node Customer Curated (write the final output as a DynamicFrame)
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-lakehouse-bucket3/customer/curated/",
        "partitionKeys": [],
        "enableUpdateCatalog": True
    },
    transformation_ctx="CustomerCurated_node3",
)

# Commit the Glue job
job.commit()
