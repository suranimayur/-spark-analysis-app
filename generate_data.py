import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_sales_data(num_records=1000000):
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Generate sample data
    data = {
        'customer_id': [f'CUST_{i:06d}' for i in range(1, num_records + 1)],
        'product_id': [f'PROD_{np.random.randint(1, 1001):04d}' for _ in range(num_records)],
        'quantity': np.random.randint(1, 11, num_records),
        'price_per_unit': np.random.uniform(10.0, 1000.0, num_records).round(2),
        'sales_date': [(datetime(2012, 1, 1) + timedelta(days=np.random.randint(0, 365))) for _ in range(num_records)],
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], num_records),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA'], num_records),
        'discount': np.random.uniform(0.0, 0.3, num_records).round(2),
        'shipping_cost': np.random.uniform(5.0, 50.0, num_records).round(2),
        'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'], num_records)
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Calculate total_sales_amount
    df['total_sales_amount'] = (df['quantity'] * df['price_per_unit'] * (1 - df['discount']) + df['shipping_cost']).round(2)
    
    # Save to CSV
    output_file = "C:\\Users\\suran\\Downloads\\python_project\\spark-analysis-app\\sales_data.csv"
    df.to_csv(output_file, index=False)
    print(f"Generated {num_records} records in {output_file}")

if __name__ == "__main__":
    generate_sales_data()
