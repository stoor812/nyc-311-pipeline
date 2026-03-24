import time
import requests   # makes HTTP calls to the API (like fetch() in JS)
import pandas as pd  # DataFrame — your main data structure
from datetime import datetime  # for timestamping the bronze filename
from pathlib import Path  # for building file paths cleanly

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def fetch_data(max_records=5000):
    offset = 0
    limit = 1000
    data = []
    while True:
        response = requests.get(API_URL, params={"$offset": offset, "$limit": limit, "$order": ":id"}, timeout=60)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        data.extend(batch)
        offset += limit
        if len(data) >= max_records:
            break
        time.sleep(0.2)
    return pd.DataFrame(data)


def save_bronze(df):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    bronze_dir = Path(__file__).parent.parent.parent / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    filename = bronze_dir / f"311_raw_{timestamp}.parquet"
    df.to_parquet(filename, index=False)
    return filename


# this block only runs when you execute the file directly
# it does NOT run if someone imports this file
if __name__ == "__main__":
    try:
        df = fetch_data()
        path = save_bronze(df)
        print(f"Saved to {path}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")