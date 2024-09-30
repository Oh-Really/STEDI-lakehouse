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

# Script generated for node Customer Trusted
CustomerTrusted_node1726231774353 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1726231774353")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726230893080 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1726230893080")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate
from ct
join al
on al.user = ct.email;
'''
SQLQuery_node1726231789094 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ct":CustomerTrusted_node1726231774353, "al":AccelerometerLanding_node1726230893080}, transformation_ctx = "SQLQuery_node1726231789094")

# Script generated for node Amazon S3
AmazonS3_node1726231886496 = glueContext.getSink(path="s3://mo-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1726231886496")
AmazonS3_node1726231886496.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1726231886496.setFormat("json")
AmazonS3_node1726231886496.writeFrame(SQLQuery_node1726231789094)
job.commit()