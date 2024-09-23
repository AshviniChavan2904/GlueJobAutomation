import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-de7ad737-e7f0-4669-b39b-7a9e0cb50a4e-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)