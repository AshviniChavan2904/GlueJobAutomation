import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parse the arguments (you can pass the job name when you run the job)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define hardcoded S3 paths
SOURCE_S3_PATH = "s3://testbucket94927/Input/"
TARGET_S3_PATH = "s3://testbucket94927/Output/"

# Read data from the specified S3 bucket
input_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": [SOURCE_S3_PATH]},  # Use the hardcoded path
    format="csv", 
    format_options={"withHeader": True}
)

# Show original data count for debugging
logger = glueContext.get_logger()
logger.info(f'Original data count: {input_data.count()}')

# Apply transformations (e.g., filtering out noisy data)
cleaned_data = Filter.apply(frame=input_data, f=lambda row: row['age'] is not None and 0 <= int(row['age']) <= 100)

# Show cleaned data count for debugging
logger.info(f'Cleaned data count: {cleaned_data.count()}')

# Write the transformed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_data, 
    connection_type="s3", 
    connection_options={"path": TARGET_S3_PATH},  # Use the hardcoded path
    format="parquet"  # Change the format as needed
)

# Commit the job
job.commit()

#Lets test