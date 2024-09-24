import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-9c1d7da4-5a07-4d86-81d9-d7c4edd1c489-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)