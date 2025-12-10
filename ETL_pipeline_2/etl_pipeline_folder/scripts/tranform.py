import os
import pandas as pd

def transform_data(raw_path):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_path)

    # -----------------------------
    # 1. Fix numeric columns
    # -----------------------------
    numeric_cols = ['tenure', 'MonthlyCharges', 'TotalCharges']

    for col in numeric_cols:
        # Convert strings like " " or "??" to NaN
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # Replace NaN with median
        df[col] = df[col].fillna(df[col].median())

    # -----------------------------
    # 2. Fix categorical columns
    # -----------------------------
    categorical_cols = [col for col in df.columns if col not in numeric_cols]

    for col in categorical_cols:
        df[col] = df[col].fillna("Unknown")

    # -----------------------------
    # 3. Feature Engineering
    # -----------------------------

    # tenure_group
    df['tenure_group'] = pd.cut(
        df['tenure'],
        bins=[0, 12, 36, 60, float('inf')],
        labels=["New", "Regular", "Loyal", "Champion"],
        include_lowest=True
    )

    # monthly_charge_segment
    df['monthly_charge_segment'] = pd.cut(
        df['MonthlyCharges'],
        bins=[-float('inf'), 30, 70, float('inf')],
        labels=["Low", "Medium", "High"]
    )

    # internet availability
    df['has_internet_service'] = df['InternetService'].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    }).fillna(0)

    # multiline
    df['is_multi_line_user'] = (df['MultipleLines'] == "Yes").astype(int)

    # contract type
    df['contract_type_code'] = df['Contract'].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    }).fillna(-1)

    # -----------------------------
    # 4. Save transformed file
    # -----------------------------
    output_path = os.path.join(staged_dir, "telco_staged.csv")
    df.to_csv(output_path, index=False)

    print(f"✅ Transformed data saved to: {output_path}")



if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
