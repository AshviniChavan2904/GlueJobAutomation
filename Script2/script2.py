import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import sum as _sum

# Parse the arguments (you can pass them when you run the job)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from a specific file in S3
input_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://testbucket94927/Input/transactions.csv"]},
    format="csv", 
    format_options={"withHeader": True}
)

# Convert DynamicFrame to Spark DataFrame
df = input_data.toDF()

# Perform transformation: Calculate the total amount spent by each customer
df_grouped = df.groupBy("customer_id").agg(_sum("transaction_amount").alias("total_spent"))

# Coalesce to 1 partition for a single output file
df_single = df_grouped.coalesce(1)

# Convert back to DynamicFrame
output_dynamic_frame = DynamicFrame.fromDF(df_single, glueContext, "output_dynamic_frame")

# Write the transformed data back to S3 as a single Parquet file
glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame, 
    connection_type="s3", 
    connection_options={"path": "s3://testbucket94927/Output/"},
    format="parquet"  # Change to parquet format
)

# Commit the job
job.commit()
