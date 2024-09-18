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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726225500428 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1726225500428")

# Script generated for node Customer Trusted
CustomerTrusted_node1726225519419 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1726225519419")

# Script generated for node SQL Query
SqlQuery0 = '''
select user,
timestamp,
x,
y,
z
from accelerometer_landing al
join customer_trusted ct ON al.user = ct.email;
'''
SQLQuery_node1726227538672 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":AccelerometerLanding_node1726225500428, "customer_trusted":CustomerTrusted_node1726225519419}, transformation_ctx = "SQLQuery_node1726227538672")

# Script generated for node Accerlerometer Trusted
AccerlerometerTrusted_node1726226212413 = glueContext.getSink(path="s3://mo-stedi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccerlerometerTrusted_node1726226212413")
AccerlerometerTrusted_node1726226212413.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccerlerometerTrusted_node1726226212413.setFormat("json")
AccerlerometerTrusted_node1726226212413.writeFrame(SQLQuery_node1726227538672)
job.commit()