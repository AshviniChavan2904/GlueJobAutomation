import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-5aa330d4-2984-4535-9e81-ba04c0fe602f-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)