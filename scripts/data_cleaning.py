import pandas as pd
import os

# Define file paths
base_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script's directory
data_dir = os.path.join(base_dir, "../Dataset/")
input_file = os.path.join(data_dir, "ecommerce_customer_data_custom_ratios.csv")
output_file = os.path.join(data_dir, "ecommerce_processed_data.csv")

# Load the dataset
df = pd.read_csv(input_file)

# Handle missing values in the Returns column
df['Returns'] = df['Returns'].fillna(0)

# Convert Purchase Date to datetime format
df['Purchase Date'] = pd.to_datetime(df['Purchase Date'])

# Calculate additional metrics
df['Average Spend per Purchase'] = df['Total Purchase Amount'] / df['Quantity']
df['Days Since Last Purchase'] = (pd.Timestamp.now() - df['Purchase Date']).dt.days
df['Loyalty Score'] = df['Quantity'] * df['Total Purchase Amount']  # Example metric

# Save the processed dataset
df.to_csv(output_file, index=False)
print(f"Processed data saved to {output_file}")
