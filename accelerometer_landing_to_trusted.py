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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1701026150239 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1701026150239",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1701098212648 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1701098212648",
)

# Script generated for node Join Customer
SqlQuery681 = """
select user, x, y, z, timestamp from accelerometer
inner join customer on customer.email = accelerometer.user
where customer.sharewithresearchasofdate is not null;
"""
JoinCustomer_node1701183304016 = sparkSqlQuery(
    glueContext,
    query=SqlQuery681,
    mapping={
        "accelerometer": AccelerometerLanding_node1701026150239,
        "customer": CustomerTrusted_node1701098212648,
    },
    transformation_ctx="JoinCustomer_node1701183304016",
)

# Script generated for node Amazon S3
AmazonS3_node1701026318892 = glueContext.getSink(
    path="s3://udacity-spark-bucket/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701026318892",
)
AmazonS3_node1701026318892.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1701026318892.setFormat("json")
AmazonS3_node1701026318892.writeFrame(JoinCustomer_node1701183304016)
job.commit()
