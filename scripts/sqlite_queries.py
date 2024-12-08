import sqlite3
import pandas as pd
import os

# Define database file path
db_file = "../Dataset/ecommerce.db"  # Path to SQLite database
output_dir = "../Dataset/sql_results"  # Directory to save query results

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Connect to the SQLite database
conn = sqlite3.connect(db_file)

# Define queries and their descriptions
queries = [
    # Revenue Analysis
    ("Total Revenue by Product Category", """
        SELECT 
            "Product Category", 
            SUM("Total Purchase Amount") AS total_revenue
        FROM 
            customers
        GROUP BY 
            "Product Category"
        ORDER BY 
            total_revenue DESC;
    """),
    ("Monthly Revenue Trend", """
        SELECT 
            STRFTIME('%Y-%m', "Purchase Date") AS month, 
            SUM("Total Purchase Amount") AS total_revenue
        FROM 
            customers
        GROUP BY 
            month
        ORDER BY 
            month;
    """),
    # Customer Behavior
    ("Top Customers by Total Spend", """
        SELECT 
            "Customer Name", 
            SUM("Total Purchase Amount") AS total_spent
        FROM 
            customers
        GROUP BY 
            "Customer Name"
        ORDER BY 
            total_spent DESC
        LIMIT 5;
    """),
    ("Customer Frequency", """
        SELECT 
            "Customer Name", 
            COUNT("Purchase Date") AS purchase_count
        FROM 
            customers
        GROUP BY 
            "Customer Name"
        ORDER BY 
            purchase_count DESC
        LIMIT 5;
    """),
    # Product Insights
    ("Top-Selling Product Categories", """
        SELECT 
            "Product Category", 
            COUNT(*) AS items_sold
        FROM 
            customers
        GROUP BY 
            "Product Category"
        ORDER BY 
            items_sold DESC
        LIMIT 5;
    """),
    ("Average Price per Product Category", """
        SELECT 
            "Product Category", 
            AVG("Product Price") AS avg_price
        FROM 
            customers
        GROUP BY 
            "Product Category"
        ORDER BY 
            avg_price DESC;
    """),
    # Churn Analysis
    ("Churned vs. Active Customers", """
        SELECT 
            "Churn", 
            COUNT(*) AS customer_count
        FROM 
            customers
        GROUP BY 
            "Churn";
    """),
    ("Average Spend of Churned vs. Active Customers", """
        SELECT 
            "Churn", 
            AVG("Total Purchase Amount") AS avg_spend
        FROM 
            customers
        GROUP BY 
            "Churn";
    """),
    # Regional Analysis
    ("Revenue by Region", """
        SELECT 
            "Geographic Location", 
            SUM("Total Purchase Amount") AS total_revenue
        FROM 
            customers
        GROUP BY 
            "Geographic Location"
        ORDER BY 
            total_revenue DESC;
    """),
    ("Top Customers by Region", """
        SELECT 
            "Geographic Location", 
            "Customer Name", 
            SUM("Total Purchase Amount") AS total_spent
        FROM 
            customers
        GROUP BY 
            "Geographic Location", "Customer Name"
        ORDER BY 
            total_spent DESC
        LIMIT 10;
    """),
    # Loyalty and Returns
    ("Average Loyalty Score by Product Category", """
        SELECT 
            "Product Category", 
            AVG("Loyalty Score") AS avg_loyalty_score
        FROM 
            customers
        GROUP BY 
            "Product Category"
        ORDER BY 
            avg_loyalty_score DESC;
    """),
    ("Revenue Lost to Returns", """
        SELECT 
            "Product Category", 
            SUM("Returns") AS total_returns,
            SUM("Returns") * AVG("Product Price") AS revenue_lost
        FROM 
            customers
        GROUP BY 
            "Product Category"
        ORDER BY 
            revenue_lost DESC;
    """),
    # Temporal Trends
    ("Sales by Day of the Week", """
        SELECT 
            STRFTIME('%w', "Purchase Date") AS day_of_week, 
            SUM("Total Purchase Amount") AS total_revenue
        FROM 
            customers
        GROUP BY 
            day_of_week
        ORDER BY 
            day_of_week;
    """),
    ("Sales by Hour of the Day", """
        SELECT 
            STRFTIME('%H', "Purchase Date") AS hour, 
            SUM("Total Purchase Amount") AS total_revenue
        FROM 
            customers
        GROUP BY 
            hour
        ORDER BY 
            hour;
    """)
]

# Execute queries and save results
for description, query in queries:
    print(f"Executing: {description}")
    result = pd.read_sql(query, conn)
    output_file = os.path.join(output_dir, f"{description.replace(' ', '_').lower()}.csv")
    result.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")

# Close the connection
conn.close()
