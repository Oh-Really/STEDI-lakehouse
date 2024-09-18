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

# Script generated for node Customer Landing
CustomerLanding_node1725015431540 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-stedi/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1725015431540")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL;
'''
SQLQuery_node1725017246023 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1725015431540}, transformation_ctx = "SQLQuery_node1725017246023")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1725017348992 = glueContext.getSink(path="s3://mo-stedi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1725017348992")
TrustedCustomerZone_node1725017348992.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1725017348992.setFormat("json")
TrustedCustomerZone_node1725017348992.writeFrame(SQLQuery_node1725017246023)
job.commit()