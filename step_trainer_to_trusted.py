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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1726497119039 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1726497119039")

# Script generated for node Customer Curated
CustomerCurated_node1726497243247 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1726497243247")

# Script generated for node SQL Query
SqlQuery0 = '''
select sensorreadingtime, stl.serialnumber, distancefromobject from stl
join cc on stl.serialnumber = cc.serialnumber;
'''
SQLQuery_node1726696676156 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"stl":StepTrainerLanding_node1726497119039, "cc":CustomerCurated_node1726497243247}, transformation_ctx = "SQLQuery_node1726696676156")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1726497428559 = glueContext.getSink(path="s3://mo-stedi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1726497428559")
StepTrainerTrusted_node1726497428559.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1726497428559.setFormat("json")
StepTrainerTrusted_node1726497428559.writeFrame(SQLQuery_node1726696676156)
job.commit()