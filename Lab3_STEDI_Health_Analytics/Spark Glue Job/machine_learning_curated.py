import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1727943185162 = glueContext.create_dynamic_frame.from_catalog(database="steadi1", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1727943185162")

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1727943185938 = glueContext.create_dynamic_frame.from_catalog(database="steadi1", table_name="step_trainer_trusted", transformation_ctx="Step_trainerTrusted_node1727943185938")

# Script generated for node Join
Join_node1727943187800 = Join.apply(frame1=Step_trainerTrusted_node1727943185938, frame2=AccelerometerTrusted_node1727943185162, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1727943187800")

# Script generated for node Drop Duplicates
DropDuplicates_node1727943196026 = DynamicFrame.fromDF(Join_node1727943187800.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1727943196026")

# Convert DynamicFrame to DataFrame and coalesce to a single partition
single_partition_df = DropDuplicates_node1727943196026.toDF().repartition(1)

# Write the DataFrame as a single JSON file
output_path = "s3://project-lakehouse-bucket3/accelerometer/curated/"
single_partition_df.write.json(output_path, mode="overwrite")

job.commit()
