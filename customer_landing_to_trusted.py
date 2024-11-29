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

# Read data from S3
customer_landing_to_trusted_node1732787032346 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://adrsparklebucket/customer/landing/"], "recurse": True},
    transformation_ctx="customer_landing_to_trusted_node1732787032346"
)

# Print schema of the source data
print("Schema of source data:")
print(customer_landing_to_trusted_node1732787032346.schema())

# Show a sample of the source data
print("Sample of source data:")
customer_landing_to_trusted_node1732787032346.show(5)

# Filter data
Filter_node1732787365605 = Filter.apply(
    frame=customer_landing_to_trusted_node1732787032346,
    f=lambda row: row.get("shareWithResearchAsOfDate", 0) != 0,
    transformation_ctx="Filter_node1732787365605"
)

# Show a sample of the filtered data
print("Sample of filtered data:")
Filter_node1732787365605.show(5)

# Show the count of rows before and after the filter
print("Total rows before filter:", customer_landing_to_trusted_node1732787032346.count())
print("Total rows after filter:", Filter_node1732787365605.count())

# Write filtered data to S3
glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1732787365605,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://adrsparklebucket/customer/trusted/test-output/"},
    transformation_ctx="CustomerTrusted_node1732787044240"
)

job.commit()
