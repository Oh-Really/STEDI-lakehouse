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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1726697298397 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1726697298397")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726697338629 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1726697338629")

# Script generated for node SQL Query
SqlQuery0 = '''
select sensorreadingtime, serialnumber, distancefromobject, at.x, at.y, at.z FROM stt
join at on stt.sensorreadingtime = at.timestamp;
'''
SQLQuery_node1726697359457 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"at":AccelerometerTrusted_node1726697338629, "stt":StepTrainerTrusted_node1726697298397}, transformation_ctx = "SQLQuery_node1726697359457")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1726697542202 = glueContext.getSink(path="s3://mo-stedi/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1726697542202")
MachineLearningCurated_node1726697542202.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1726697542202.setFormat("json")
MachineLearningCurated_node1726697542202.writeFrame(SQLQuery_node1726697359457)
job.commit()