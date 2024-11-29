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

# Script generated for node CustomCurated
CustomCurated_node1732852018105 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/customer/curated/"], "recurse": True}, transformation_ctx="CustomCurated_node1732852018105")

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1732852015788 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1732852015788")

# Script generated for node Join
Join_node1732852023297 = Join.apply(frame1=CustomCurated_node1732852018105, frame2=StepTrainerLanding_node1732852015788, keys1=["serialNumber"], keys2=["serialNumber"], transformation_ctx="Join_node1732852023297")

# Script generated for node Change Schema
ChangeSchema_node1732852063437 = ApplyMapping.apply(frame=Join_node1732852023297, mappings=[("serialNumber", "string", "serialNumber", "string"), ("`.serialNumber`", "string", "`.serialNumber`", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="ChangeSchema_node1732852063437")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1732852094282 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732852063437, connection_type="s3", format="json", connection_options={"path": "s3://adrsparklebucket/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1732852094282")

job.commit()