
import sys
import io
from datetime import datetime
from extract import fetch_all_cities
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if __name__ == "__main__":
    print("🚀 URBAN AIR DATA – PIPELINE RUNNER")
    print(f"Run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    cities_env = ['Delhi', 'Bengaluru', 'Hyderabad', 'Mumbai', 'Kolkata']
    print("========== EXTRACT START ==========")
    try:
        res = fetch_all_cities(cities_env)
        print("✅ Extract SUCCESSFUL")
    except Exception as e:
        print(f"❌ Extract FAILED: {e}")
        sys.exit(1)