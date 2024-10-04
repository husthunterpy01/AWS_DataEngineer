import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AccelerometerLanding_node
AccelerometerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lakehouse-bucket3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node",
)

# Script generated for node CustomerTrusted_node
CustomerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://project-lakehouse-bucket3/customer/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node",
)

# Script generated for node CustomerPrivacyFilter_node
CustomerPrivacyFilter_node = Join.apply(
    frame1=AccelerometerLanding_node,
    frame2=CustomerTrusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Script generated for node DropFields_node
DropFields_node = ApplyMapping.apply(
    frame=CustomerPrivacyFilter_node,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "float"),
        ("y", "double", "y", "float"),
        ("z", "double", "z", "float"),
    ],
    transformation_ctx="DropFields_node",
)

# Script generated for node AccelerometerTrusted_nodes
AccelerometerTrusted_nodes = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-lakehouse-bucket3/accelerometer/trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
    },
    transformation_ctx="AccelerometerTrusted_nodes",
)

job.commit()