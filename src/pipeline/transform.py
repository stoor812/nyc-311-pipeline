import pandas as pd
from pathlib import Path
from datetime import datetime

DATE_COLUMNS = [
    "created_date",
    "closed_date",
    "resolution_action_updated_date",
]

FLOAT_COLUMNS = [
    "latitude",
    "longitude",
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
]

DROP_COLUMNS = [
    "due_date",
    "bridge_highway_name",
    "bridge_highway_direction",
    "bridge_highway_segment",
    "road_ramp",
    "taxi_pick_up_location",
    "location",
]

def transform_data(df):
    df = df.copy()
    # cast date columns to datetime
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # cast numeric columns to float
    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # drop useless columns
    df = df.drop(columns=[col for col in DROP_COLUMNS if col in df.columns])

    return df

def save_silver(df):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    silver_dir = Path(__file__).parent.parent.parent / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    filename = silver_dir / f"311_silver_{timestamp}.parquet"
    df.to_parquet(filename, index=False)
    return filename


if __name__ == "__main__":
    # read latest bronze file
    bronze_dir = Path(__file__).parent.parent.parent / "data" / "bronze"
    files = sorted(bronze_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No bronze files found. Run ingest.py first.")
    latest = files[-1]
    print(f"Reading: {latest}")

    df = pd.read_parquet(latest)
    df = transform_data(df)
    path = save_silver(df)
    print(f"Saved silver to: {path}")