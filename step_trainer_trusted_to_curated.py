import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1687094408884 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1687094408884",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1687094412204 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1687094412204",
)

# Script generated for node Join
Join_node1687096022717 = Join.apply(
    frame1=AWSGlueDataCatalog_node1687094412204,
    frame2=AWSGlueDataCatalog_node1687094408884,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1687096022717",
)

# Script generated for node Drop Fields
DropFields_node1687096069726 = DropFields.apply(
    frame=Join_node1687096022717,
    paths=["user", "y", "timestamp", "z", "x"],
    transformation_ctx="DropFields_node1687096069726",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1687096062235 = DynamicFrame.fromDF(
    DropFields_node1687096069726.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1687096062235",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-data-nam/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1687096062235)
job.commit()
