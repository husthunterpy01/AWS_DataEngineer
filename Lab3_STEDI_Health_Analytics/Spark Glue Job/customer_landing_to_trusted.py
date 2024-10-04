import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerLanding_node
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://project-lakehouse-bucket3/customer/landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node",
)

# Script generated for node PrivacyFilter_node
PrivacyFilter_node = Filter.apply(
    frame=CustomerLanding_node,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node",
)

# Script generated for node CustomerTrusted_node
CustomerTrusted_node = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://project-lakehouse-bucket3/customer/trusted/", 
                        "partitionKeys": [], 
                        "enableUpdateCatalog": True},
    transformation_ctx="CustomerTrusted_node",
)

job.commit()