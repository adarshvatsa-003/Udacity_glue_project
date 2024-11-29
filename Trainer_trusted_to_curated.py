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

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1732853124229 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/step_trainer/trusted/run-1732852936670-part-r-00000"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1732853124229")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1732853140507 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://adrsparklebucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1732853140507")

# Script generated for node Join
Join_node1732853143086 = Join.apply(frame1=AccelerometerTrusted_node1732853140507, frame2=StepTrainerTrusted_node1732853124229, keys1=["timestamp"], keys2=["distancefromobject"], transformation_ctx="Join_node1732853143086")

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1732853145080 = glueContext.write_dynamic_frame.from_options(frame=Join_node1732853143086, connection_type="s3", format="json", connection_options={"path": "s3://adrsparklebucket/MlCurated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1732853145080")

job.commit()