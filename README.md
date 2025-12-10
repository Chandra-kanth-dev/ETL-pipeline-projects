# ETL-pipeline-projects
📘 Telco Customer Churn – ETL & Analysis Project
📌 Overview

This project implements a complete ETL (Extract–Transform–Load) pipeline for the Telco Customer Churn dataset, along with automated data cleaning, Supabase table maintenance, and analytical report generation.

It includes:

📥 Extraction of raw CSV data

🔄 Transformation & cleaning logic

🗄️ Loading into Supabase

🧹 Automatic removal of empty columns

📊 Analysis report generation

📁 Export of processed results

🛠️ Project Structure
project_root/
│
├── data/
│   ├── raw/
│   │   └── WA_Fn-UseC_-Telco-Customer-Churn.csv
│   └── processed/
│       └── analysis_summary.csv
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── etl_analysis.py
│
└── README.md

📥 1. Extract — extract.py

The script locates and returns the absolute path of the raw Telco churn CSV file.

✔ What it does

Identifies the project root directory.

Searches in data/raw/.

Returns full file path.

Throws error if file missing.

🔄 2. Transform — transform.py

Cleans and prepares the Telco dataset:

Converts data types

Fills missing values

Drops duplicates

Removes invalid rows (e.g., blank TotalCharges)

🗄️ 3. Load — load.py

Loads the cleaned dataset into Supabase using PostgREST API.

Includes:

Insert records

Optional table reset

Optional schema enforcement

🧹 4. Table Cleanup in Supabase
✔ Delete all rows
DELETE FROM telco_data;

✔ Remove duplicate rows (no id column needed)
DELETE FROM telco_data t1
USING telco_data t2
WHERE t1.ctid < t2.ctid
AND t1.* = t2.*;

✔ Remove columns where all values are NULL
DO $$
DECLARE
    col RECORD;
    tbl text := 'telco_data';
    col_name text;
    cnt int;
BEGIN
    FOR col IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = tbl
    LOOP
        col_name := col.column_name;
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE %I IS NOT NULL', tbl, col_name) INTO cnt;
        IF cnt = 0 THEN
            EXECUTE format('ALTER TABLE %I DROP COLUMN %I', tbl, col_name);
            RAISE NOTICE 'Dropped column: %', col_name;
        END IF;
    END LOOP;
END$$;

📊 5. Analysis — etl_analysis.py

Generates full analytics report from Supabase table.

Metrics Included:

Churn percentage

Avg Monthly Charges per Contract Type

Count of:

New customers

Regular customers

Loyal customers

Champion customers

Internet service distribution

Pivot: Churn vs Tenure Group

Optional visualizations:

Churn rate by Monthly Charge Segment

Histogram: TotalCharges

Contract Type Bar Plot

Output:

Saved in:

data/processed/analysis_summary.csv

▶️ Running the Entire Pipeline

To run ETL + analysis:

python src/extract.py
python src/transform.py
python src/load.py
python src/etl_analysis.py


Or create one master script:

python run_pipeline.py

🚀 Future Improvements

Automate Supabase schema enforcement

Add model training (Churn Prediction)

Deploy API + dashboard

Integrate Streamlit dashboards
🚢 Titanic Dataset – ETL & Analysis Pipeline
📌 Overview

This project implements a complete ETL (Extract–Transform–Load) and Analytics pipeline for the Titanic Passenger Dataset, integrated with a Supabase PostgreSQL database.

It includes:

📥 Extracting Titanic CSV data

🔄 Cleaning & transforming raw passenger records

🗄️ Loading cleaned data into Supabase

🧹 Removing empty columns from Supabase table

🧼 Removing duplicate rows

📊 Automated analysis report generation

📁 Export of summarized metrics

🛠️ Project Structure
project_root/
│
├── data/
│   ├── raw/
│   │   └── titanic.csv
│   └── processed/
│       └── analysis_summary.csv
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── analysis.py
│
└── README.md

1️⃣ Extract — extract.py

Reads the Titanic CSV from the data/raw/ folder.

✔ Features

Automatically locates project root

Validates if file exists

Returns absolute path

2️⃣ Transform — transform.py

Cleans and preprocesses the Titanic data.

✔ Operations

Converts datatypes (Age, Fare → numeric)

Handles missing values (Age, Cabin, Embarked)

Drops duplicates

Standardizes column names (optional)

Removes inconsistent or invalid records

3️⃣ Load — load.py

Loads cleaned Titanic data into Supabase.

✔ Features

Inserts batch records into your Supabase table

Supports table reset

Optional schema validation

4️⃣ Supabase Table Maintenance
🧹 Remove ALL duplicate rows

(works even if no id column exists)

DELETE FROM titanic_data t1
USING titanic_data t2
WHERE t1.ctid < t2.ctid
AND t1.* = t2.*;

🧹 Remove columns where ALL values are NULL
DO $$
DECLARE
    col RECORD;
    tbl text := 'titanic_data';
    cnt int;
BEGIN
    FOR col IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = tbl
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM %I WHERE %I IS NOT NULL',
            tbl, col.column_name
        ) INTO cnt;

        IF cnt = 0 THEN
            EXECUTE format(
                'ALTER TABLE %I DROP COLUMN %I',
                tbl, col.column_name
            );
            RAISE NOTICE 'Dropped column: %', col.column_name;
        END IF;
    END LOOP;
END$$;

5️⃣ Analysis — analysis.py

Reads the cleaned Titanic data from Supabase and generates analytics.

📊 Metrics Generated

Survival rate

Survival % by gender

Survival % by passenger class (Pclass)

Average age of survivors vs non-survivors

Fare statistics

Embarkation distribution

Family size & survival correlation

Pivot table: Survival vs Pclass

OPTIONAL charts:

Age distribution histogram

Fare distribution histogram

Survival rate by gender bar chart

📁 Output saved to:
data/processed/analysis_summary.csv

▶️ Running the Entire Pipeline

Run step-by-step:

python src/extract.py
python src/transform.py
python src/load.py
python src/analysis.py


Or combine into one script:

python run_pipeline.py

📦 Requirements

Python 3.8+

pandas

numpy

supabase-py

matplotlib (optional for plots)

python-dotenv

A sample requirements.txt can be generated if you want it.

🚀 Future Extensions

Train ML survival prediction model

Deploy model using FastAPI

Add Streamlit dashboard

Add automated Supabase triggers for updates
