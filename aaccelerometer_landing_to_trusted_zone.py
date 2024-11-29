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
CustomerTrusted_node1732824834986 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1732824834986")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1732824836060 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1732824836060")

# Script generated for node Join
Join_node1732824841658 = Join.apply(frame1=AccelerometerLanding_node1732824836060, frame2=CustomerTrusted_node1732824834986, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1732824841658")

# Script generated for node Change Schema
ChangeSchema_node1732824881432 = ApplyMapping.apply(frame=Join_node1732824841658, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double"), ("serialnumber", "string", "serialnumber", "string"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("birthday", "string", "birthday", "string"), ("registrationdate", "long", "registrationdate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("customername", "string", "customername", "string"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long")], transformation_ctx="ChangeSchema_node1732824881432")

# Script generated for node Amazon S3
AmazonS3_node1732824904187 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732824881432, connection_type="s3", format="json", connection_options={"path": "s3://adrsparklebucket/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1732824904187")

job.commit()