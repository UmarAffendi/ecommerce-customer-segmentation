import sqlite3
import pandas as pd
import os

# Define file paths
base_dir = os.path.dirname(os.path.abspath(__file__))  # Current script directory
data_dir = os.path.join(base_dir, "../Dataset/")  # Path to data folder
csv_file = os.path.join(data_dir, "ecommerce_processed_data.csv")
db_file = os.path.join(data_dir, "ecommerce.db")  # SQLite database file

# Load the processed CSV into a DataFrame
df = pd.read_csv(csv_file)

# Create a SQLite database and connect to it
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create a table and load the data into SQLite
df.to_sql("customers", conn, if_exists="replace", index=False)
print(f"Database created and data loaded into 'customers' table: {db_file}")

# Verify the data was loaded (example query)
query = "SELECT * FROM customers LIMIT 5"
result = pd.read_sql(query, conn)
print(result)

# Close the connection
conn.close()
