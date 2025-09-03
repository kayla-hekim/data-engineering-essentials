import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Replace with the actual file path
csv_file = 'SAU-GLOBAL-1-v48-0.csv'
parquet_file = 'converted-SAU-GLOBAL-1-v48-0.csv.parquet'  # suffix doesn't matter in here, only convenient for humans

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(csv_file)

# Convert the DataFrame to a PyArrow Table
table = pa.Table.from_pandas(df)

# Write the PyArrow Table to a Parquet file
pq.write_table(table, parquet_file)

print(f"Successfully converted {csv_file} to {parquet_file}")
