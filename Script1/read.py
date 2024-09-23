import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-206e103f-aeca-4142-9fd5-38e101898824-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)