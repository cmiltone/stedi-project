import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1701097552221 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1701097552221",
)

# Script generated for node Filter Customer
# drops rows that do not have data in the sharedWithResearchAsOfDate column
SqlQuery749 = """
select * from myDataSource
WHERE sharewithresearchasofdate IS NOT NULL;
"""
FilterCustomer_node1701097415099 = sparkSqlQuery(
    glueContext,
    query=SqlQuery749,
    mapping={"myDataSource": CustomerLanding_node1701097552221},
    transformation_ctx="FilterCustomer_node1701097415099",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1701097754217 = glueContext.getSink(
    path="s3://udacity-spark-bucket/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1701097754217",
)
CustomerTrusted_node1701097754217.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="customer_trusted"
)
CustomerTrusted_node1701097754217.setFormat("json")
CustomerTrusted_node1701097754217.writeFrame(FilterCustomer_node1701097415099)
job.commit()
