import os
import sys
sys.path.append(r"D:\pharma_pipeline")

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

from config.settings import (
    RAW_DIR, PRODUCTS, SALES_TEAMS, 
    SUBJECT_AREAS, ZIP_CODES, ACCOUNT_IDS,
    START_DATE, END_DATE
)

fake = Faker()
random.seed(42)
np.random.seed(42)

def generate_sales_data(num_records: int = 10000) -> pd.DataFrame:
    """
    Generate synthetic pharma sales records mimicking
    Symphony/ValueCentric subject area structure.
    """
    
    # Flatten product list
    all_products = []
    for category, products in PRODUCTS.items():
        for product in products:
            all_products.append((category, product))
    
    records = []
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    date_range = (end - start).days

    for _ in range(num_records):
        category, product = random.choice(all_products)
        subject_area = random.choice(SUBJECT_AREAS)
        sales_team = random.choice(SALES_TEAMS)
        zip_code = random.choice(ZIP_CODES)
        account_id = random.choice(ACCOUNT_IDS)
        
        # Random date
        sale_date = start + timedelta(days=random.randint(0, date_range))
        
        # Realistic sales figures with some noise
        base_sales = random.uniform(1000, 50000)
        units = random.randint(1, 100)
        
        records.append({
            "record_id": fake.uuid4(),
            "sale_date": sale_date.strftime("%Y-%m-%d"),
            "product": product,
            "category": category,
            "subject_area": subject_area,
            "sales_team": sales_team,
            "zip_code": zip_code,
            "account_id": account_id,
            "units_sold": units,
            "sales_amount": round(base_sales, 2),
            "revenue": round(base_sales * units * random.uniform(0.8, 1.2), 2),
            "region": fake.state_abbr(),
            "rep_id": f"REP{random.randint(100, 999)}"
        })
    
    return pd.DataFrame(records)


def save_raw_data(df: pd.DataFrame, filename: str = "pharma_sales_raw.csv"):
    os.makedirs(RAW_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"Raw data saved to: {filepath}")
    print(f"Shape: {df.shape}")
    print(f"\nSample:")
    print(df.head(3).to_string())
    return filepath


if __name__ == "__main__":
    print("Generating synthetic pharma sales data...")
    df = generate_sales_data(num_records=10000)
    save_raw_data(df)