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

# Script generated for node Customer Trusted
CustomerTrusted_node1701103739991 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1701103739991",
)

# Script generated for node Customer Curated
CustomerCurated_node1701104389666 = glueContext.getSink(
    path="s3://udacity-spark-bucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1701104389666",
)
CustomerCurated_node1701104389666.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_curated"
)
CustomerCurated_node1701104389666.setFormat("json")
CustomerCurated_node1701104389666.writeFrame(CustomerTrusted_node1701103739991)
job.commit()
