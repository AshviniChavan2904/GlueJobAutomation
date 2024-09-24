import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-f93bae9f-73a7-4859-953c-2b489cad79ec-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)