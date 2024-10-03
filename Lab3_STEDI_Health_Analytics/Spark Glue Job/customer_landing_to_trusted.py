import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Retrieve the job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the data from the S3 Customer Landing zone (JSON format)
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "true"}, 
    connection_type="s3", 
    format="json", 
    connection_options={"paths": ["s3://project-lakehouse-bucket3/customer/landing/"], "recurse": True}, 
    transformation_ctx="CustomerLanding_node"
)

# Apply the filter transformation to remove rows with 'shareWithResearchAsOfDate' as 0
Filtered_node = Filter.apply(
    frame=CustomerLanding_node, 
    f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), 
    transformation_ctx="Filtered_node"
)

# Convert the dynamic frame to a Spark DataFrame for further transformations
df = Filtered_node.toDF()

# Coalesce the DataFrame to a single partition to ensure output is written as a single file
df_single_partition = df.coalesce(1)

# Write the result as a single uncompressed JSON file to the S3 'Trusted' zone
df_single_partition.write.mode('overwrite').json("s3://project-lakehouse-bucket3/customer/trusted/")

# Commit the Glue job
job.commit()
