import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job name from the arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from the "Accelerometer Landing" table
AccelerometerLanding_node = glueContext.create_dynamic_frame.from_catalog(
    database="steadi1", 
    table_name="accelerometer_landing", 
    transformation_ctx="AccelerometerLanding_node"
)

# Load data from the "Customer Trusted" table
CustomerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="steadi1", 
    table_name="customer_trusted", 
    transformation_ctx="CustomerTrusted_node"
)

# Join the two datasets on "user" and "email"
Joined_node = Join.apply(
    frame1=AccelerometerLanding_node, 
    frame2=CustomerTrusted_node, 
    keys1=["user"], 
    keys2=["email"], 
    transformation_ctx="Joined_node"
)

# Select specific fields to output
SelectedFields_node = SelectFields.apply(
    frame=Joined_node, 
    paths=["user", "timestamp", "x", "y", "z"], 
    transformation_ctx="SelectedFields_node"
)

# Convert the DynamicFrame to a Spark DataFrame
df = SelectedFields_node.toDF()

# Coalesce the DataFrame into a single partition to write a single JSON file
df_single_partition = df.coalesce(1)

# Write the output as a single uncompressed JSON file to the S3 bucket
df_single_partition.write.mode('overwrite').json("s3://project-lakehouse-bucket3/accelerometer/trusted/")

# Commit the job
job.commit()
