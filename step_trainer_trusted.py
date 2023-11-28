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

# Script generated for node Customer Curated
CustomerCurated_node1701105575893 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1701105575893",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1701105457515 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1701105457515",
)

# Script generated for node SQL Query
SqlQuery756 = """
select
  `step_trainer`.`sensorreadingtime`, `step_trainer`.`serialNumber`, `step_trainer`.`distanceFromObject`
from
  `step_trainer`
   inner join `customer` on `step_trainer`.`serialnumber` = `customer`.`serialnumber`
"""
SQLQuery_node1701105662962 = sparkSqlQuery(
    glueContext,
    query=SqlQuery756,
    mapping={
        "step_trainer": StepTrainerLanding_node1701105457515,
        "customer": CustomerCurated_node1701105575893,
    },
    transformation_ctx="SQLQuery_node1701105662962",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1701105380780 = glueContext.getSink(
    path="s3://udacity-spark-bucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1701105380780",
)
StepTrainerTrusted_node1701105380780.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1701105380780.setFormat("json")
StepTrainerTrusted_node1701105380780.writeFrame(SQLQuery_node1701105662962)
job.commit()
