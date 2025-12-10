# File: extract.py
"""
extract.py  
Purpose: Return absolute path to the raw Telco CSV file.
Assumes the file name is: WA_Fn-UseC_-Telco-Customer-Churn.csv
Location: <project_root>/data/raw/
"""

import os

def extract_data():
    """
    Return absolute path to the raw Telco CSV file.

    Returns
    -------
    str
        Absolute path to data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv
    """
    # Directory of this script, e.g. <project_root>/src
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # project root = one level above script directory
    project_root = os.path.dirname(script_dir)

    raw_path = os.path.join(
        project_root,
        "data",
        "raw",
        "WA_Fn-UseC_-Telco-Customer-Churn.csv"
    )

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    print(f"📌 Raw file path: {raw_path}")
    return raw_path


if __name__ == "__main__":
    extract_data()
