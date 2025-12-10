# File: load.py
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
# Only these columns will exist in DB
# ------------------------------------------------------
ALLOWED_COLUMNS = [
    "tenure", "MonthlyCharges", "TotalCharges", "Churn", "InternetService",
    "Contract", "PaymentMethod", "tenure_group", "monthly_charge_segment",
    "has_internet_service", "is_multi_line_user", "contract_type_code"
]

def create_table_if_not_exists():
    try:
        supabase = get_supabase_client()
        create_table_sql = """
CREATE TABLE IF NOT EXISTS telco_data (
    id BIGSERIAL PRIMARY KEY,
    tenure INT,
    "MonthlyCharges" FLOAT,
    "TotalCharges" FLOAT,
    "Churn" TEXT,
    "InternetService" TEXT,
    "Contract" TEXT,
    "PaymentMethod" TEXT,
    tenure_group TEXT,
    monthly_charge_segment TEXT,
    has_internet_service INT,
    is_multi_line_user INT,
    contract_type_code INT
);
"""
        try:
            supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
            print("✅ Table 'telco_data' is ready.")
        except Exception as e:
            print(f"⚠️ RPC error: {e}")
            print("➡️ Please create RPC function manually:\n\n"
                  "CREATE OR REPLACE FUNCTION execute_sql(query text)\n"
                  "RETURNS text LANGUAGE plpgsql SECURITY definer AS $$\n"
                  "BEGIN EXECUTE query; RETURN 'OK'; END; $$;")
    except Exception as e:
        print(f"❌ Table creation error: {e}")
def auto_add_missing_columns(supabase, table_name, df_columns):
    try:
        first_row = supabase.table(table_name).select("*").limit(1).execute()
        supabase_columns = list(first_row.data[0].keys()) if first_row.data else []
        missing_columns = [col for col in df_columns if col in ALLOWED_COLUMNS and col not in supabase_columns]
        for col in missing_columns:
            sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
            if isinstance(sample_val, int):
                col_type = "INT"
            elif isinstance(sample_val, float):
                col_type = "FLOAT"
            else:
                col_type = "TEXT"
            alter_sql = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {col_type};'
            try:
                supabase.rpc("execute_sql", {"query": alter_sql}).execute()
                print(f"🛠 Added missing column: {col} ({col_type})")
            except Exception as e:
                print(f"⚠️ Failed to add column {col}: {e}")
    except Exception as e:
        print(f"❌ Auto alter table error: {e}")


def load_to_supabase(staged_path: str, table_name: str = "telco_data"):
    staged_path = os.path.abspath(staged_path)
    print(f"📂 Reading CSV: {staged_path}")
    if not os.path.exists(staged_path):
        print(f" CSV not found: {staged_path}")
        return   
    try:
        supabase = get_supabase_client()
        global df
        df = pd.read_csv(staged_path)
        df = df.where(pd.notnull(df), None)
        df = df[[col for col in df.columns if col in ALLOWED_COLUMNS]]
        total_rows = len(df)
        auto_add_missing_columns(supabase, table_name, df.columns)

        first_row = supabase.table(table_name).select("*").limit(1).execute()
        supabase_columns = list(first_row.data[0].keys()) if first_row.data else list(df.columns)

        batch_size = 200
        for start in range(0, total_rows, batch_size):
            batch = df.iloc[start:start + batch_size].copy()
            cleaned_records = []
            for _, row in batch.iterrows():
                cleaned = {col: row[col] for col in supabase_columns if col in batch.columns}
                cleaned_records.append(cleaned)
            try:
                supabase.table(table_name).insert(cleaned_records).execute()
                print(f"✅ Inserted rows {start + 1} – {min(start + batch_size, total_rows)}")
            except Exception as e:
                print(f"⚠️ Batch insert failed: {e}")

        print("🎉 DONE — All rows uploaded successfully!")
    except Exception as e:
        print(f"❌ Load error: {e}")
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "telco_staged.csv")
    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
    
