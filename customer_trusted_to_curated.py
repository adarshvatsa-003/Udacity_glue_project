import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node CustomerTrusted
CustomerTrusted_node1732822811075 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/customer/trusted/test-output/part-00000-3e9f2391-c28b-4f69-9980-2183ab0e8941-c000.json"], "recurse": True}, transformation_ctx="CustomerTrusted_node1732822811075")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1732822755423 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1732822755423")

# Script generated for node Join
Join_node1732822821536 = Join.apply(frame1=CustomerTrusted_node1732822811075, frame2=AccelerometerLanding_node1732822755423, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1732822821536")

# Script generated for node Change Schema
ChangeSchema_node1732823348361 = ApplyMapping.apply(frame=Join_node1732822821536, mappings=[("serialNumber", "string", "serialNumber", "string"), ("birthDay", "string", "birthDay", "string"), ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"), ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "registrationDate", "long"), ("customerName", "string", "customerName", "string"), ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"), ("timestamp", "long", "timestamp", "long"), ("email", "string", "email", "string"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1732823348361")

# Script generated for node CustomerCurated
CustomerCurated_node1732822897188 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732823348361, connection_type="s3", format="json", connection_options={"path": "s3://adrsparklebucket/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1732822897188")

job.commit()