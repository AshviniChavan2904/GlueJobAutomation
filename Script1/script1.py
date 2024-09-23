import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parse the arguments (you can pass them when you run the job)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from an S3 bucket (change the S3 path to your bucket and file)
input_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://testbucket94927/Input/"]}, 
    format="csv", 
    format_options={"withHeader": True}
)

# Apply transformations (e.g., filtering data where a column 'age' is greater than 25)
filtered_data = Filter.apply(frame=input_data, f=lambda row: int(row['age']) > 25)

# Write the transformed data back to S3 as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=filtered_data, 
    connection_type="s3", 
    connection_options={"path": "s3://testbucket94927/Output/"},
    format="parquet"
)

# Commit the job
job.commit()