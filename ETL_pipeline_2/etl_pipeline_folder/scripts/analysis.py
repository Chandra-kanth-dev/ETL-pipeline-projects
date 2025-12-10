# File: etl_analysis.py
"""
ETL Analysis Script
Reads data from Supabase table 'telco_data', performs analysis, 
and saves a summary CSV to data/processed/analysis_summary.csv
"""

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# ------------------------------------------------------
# Supabase client
# ------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)

# ------------------------------------------------------
# Read table from Supabase
# ------------------------------------------------------
def read_table(table_name: str) -> pd.DataFrame:
    supabase = get_supabase_client()
    response = supabase.table(table_name).select("*").execute()
    
    # Convert response to DataFrame
    df = pd.DataFrame(response.data)
    
    if df.empty:
        print(f"⚠️ Table '{table_name}' is empty.")
    else:
        print(f"✅ Retrieved {len(df)} rows from '{table_name}'.")
    
    return df

# ------------------------------------------------------
# Perform analysis
# ------------------------------------------------------
def perform_analysis():
    df = read_table("telco_data")
    if df.empty:
        return
    
    # 1️⃣ Churn percentage
    churn_percentage = df['Churn'].value_counts(normalize=True) * 100
    
    # 2️⃣ Average MonthlyCharges per Contract
    avg_monthly_charges = df.groupby('Contract')['MonthlyCharges'].mean()
    
    # 3️⃣ Count of new, regular, loyal, champion customers (example: based on tenure)
    def customer_segment(tenure):
        if tenure <= 12:
            return "New"
        elif tenure <= 24:
            return "Regular"
        elif tenure <= 48:
            return "Loyal"
        else:
            return "Champion"
    
    df['CustomerSegment'] = df['tenure'].apply(customer_segment)
    segment_counts = df['CustomerSegment'].value_counts()
    
    # 4️⃣ Internet service distribution
    internet_dist = df['InternetService'].value_counts()
    
    # 5️⃣ Pivot table: Churn vs Tenure Group
    if 'tenure_group' in df.columns:
        churn_vs_tenure = pd.pivot_table(df, index='tenure_group', columns='Churn', aggfunc='size', fill_value=0)
    else:
        churn_vs_tenure = pd.DataFrame()

    output_dir = os.path.join("..", "data", "processed")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "analysis_summary.csv")
    
    # Combine all metrics into one DataFrame for CSV
    summary = pd.DataFrame({
        "ChurnPercentage": churn_percentage,
        "AvgMonthlyCharges": avg_monthly_charges.reindex(churn_percentage.index, fill_value=0),
        "SegmentCounts": segment_counts.reindex(churn_percentage.index, fill_value=0),
        "InternetServiceDist": internet_dist.reindex(churn_percentage.index, fill_value=0)
    }).fillna(0)
    
    summary.to_csv(output_path, index=True)
    print(f"🎉 Analysis summary saved to: {output_path}")

# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    perform_analysis()
