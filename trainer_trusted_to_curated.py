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
AWSGlueDataCatalog_node1686782113524 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1686782113524",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-nam/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1686782176203 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AWSGlueDataCatalog_node1686782113524,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1686782176203",
)

# Script generated for node Drop Fields
DropFields_node1687016884505 = DropFields.apply(
    frame=Join_node1686782176203,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1687016884505",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1687024478903 = DynamicFrame.fromDF(
    DropFields_node1687016884505.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1687024478903",
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
    catalogDatabase="stedi-db", catalogTableName="step_trainer_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1687024478903)
job.commit()
