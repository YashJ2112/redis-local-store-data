import datatable as dt
import redis
import pandas as pd
from datetime import datetime

# Load the data into a DataTable object
data = dt.fread("data.csv")

# Convert DataTable to a list of dictionaries
df = data.to_pandas()

df = df.head(100)

# Connect to Redis locally
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

rs = r.ft("idx:cleaned_data")

# Upload the data to Redis
for _, row in df.iterrows():
    row_id = row['key']  # Assuming 'key' column contains the unique identifier
    row_dict = row.drop('key').to_dict()
    
    # Convert Timestamp values to string
    for key, value in row_dict.items():
        if isinstance(value, pd.Timestamp):
            row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
    
    r.hset(row_id, mapping=row_dict)

print("Data uploaded to Redis successfully.")

# # Test the data retrieval from Redis
keys = r.keys("*")
for key in keys:
    values = r.hgetall(key)
    print(f"Key: {key}, Values: {values}")