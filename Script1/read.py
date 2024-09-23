import pandas as pd

# Read the Parquet file
df = pd.read_parquet('part-00000-c516386c-0f10-4b8c-8649-c9ad65c138b3-c000.snappy.parquet')

# Show the contents of the Parquet file
print(df)