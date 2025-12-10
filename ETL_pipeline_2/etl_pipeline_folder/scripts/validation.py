'''
Docstring for etl_pipeline_folder.scripts.validation
VALIDATION SCRIPT (validate.py)
After load, write a script that checks:
No missing values in:
tenure, MonthlyCharges, TotalCharges
Unique count of rows = original dataset
Row count matches Supabase table
All segments (tenure_group, monthly_charge_segment) exist
Contract codes are only {0,1,2}
Print a validation summary.
 '''# File: validation.py
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)
def validate_data(original_csv_path: str, table_name: str = "telco_data"):
    original_csv_path = os.path.abspath(original_csv_path)
    if not os.path.exists(original_csv_path):
        print(f"❌ Original CSV not found: {original_csv_path}")
        return
    df_original = pd.read_csv(original_csv_path)
    supabase = get_supabase_client()
    response = supabase.table(table_name).select("*").execute()
    df_loaded = pd.DataFrame(response.data)
    print("📊 Validation Summary:")
    # 1. No missing values in key columns
    key_columns = ['tenure', 'MonthlyCharges', 'TotalCharges']
    for col in key_columns:
        missing_count = df_loaded[col].isnull().sum()
        if missing_count == 0:
            print(f"✅ No missing values in '{col}'")
        else:
            print(f"❌ Missing values found in '{col}': {missing_count}")
    # 2. Unique count of rows matches original dataset
    original_unique_count = len(df_original.drop_duplicates())
    loaded_unique_count = len(df_loaded.drop_duplicates())
    if original_unique_count == loaded_unique_count:
        print(f"✅ Unique row count matches: {original_unique_count}")
    else:
        print(f"❌ Unique row count mismatch: Original={original_unique_count}, Loaded={loaded_unique_count}")
    # 3. Row count matches Supabase table
    original_row_count = len(df_original)
    loaded_row_count = len(df_loaded)
    if original_row_count == loaded_row_count:
        print(f"✅ Row count matches: {original_row_count}")
    else:
        print(f"❌ Row count mismatch: Original={original_row_count}, Loaded={loaded_row_count}")
    # 4. All segments exist
    tenure_groups = set(df_loaded['tenure_group'].unique())
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    if tenure_groups == expected_tenure_groups:
        print("✅ All tenure groups exist")
    else:
        print(f"❌ Missing tenure groups: Expected={expected_tenure_groups}, Found={tenure_groups}")
    monthly_segments = set(df_loaded['monthly_charge_segment'].unique())
    expected_monthly_segments = {"Low", "Medium", "High"}
    if monthly_segments == expected_monthly_segments:
        print("✅ All monthly charge segments exist")
    else:
        print(f"❌ Missing monthly charge segments: Expected={expected_monthly_segments}, Found={monthly_segments}")

    contract_codes = set(df_loaded['contract_type_code'].unique())
    expected_contract_codes = {0, 1, 2}
    if contract_codes.issubset(expected_contract_codes):
        print("✅ Contract type codes are valid")
    else:
        print(f"❌ Invalid contract type codes found: {contract_codes - expected_contract_codes}")
if __name__ == "__main__":
    original_csv = os.path.join("..", "data", "raw", "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    validate_data(original_csv)

