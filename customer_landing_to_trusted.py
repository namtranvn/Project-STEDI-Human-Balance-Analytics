import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-nam/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Filter
Filter_node1686664582777 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1686664582777",
)

# Script generated for node Change Schema
ChangeSchema_node1686777580417 = ApplyMapping.apply(
    frame=Filter_node1686664582777,
    mappings=[
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "bigint"),
        ("birthDay", "string", "birthDay", "date"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("registrationDate", "long", "registrationDate", "timestamp"),
        ("lastUpdateDate", "long", "lastUpdateDate", "timestamp"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "timestamp"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "timestamp"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "timestamp"),
    ],
    transformation_ctx="ChangeSchema_node1686777580417",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686844574680 = DynamicFrame.fromDF(
    ChangeSchema_node1686777580417.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1686844574680",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-data-nam/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1686844574680)
job.commit()
