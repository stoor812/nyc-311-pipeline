import pandas as pd
from pathlib import Path

VALID_BOROUGHS = {
    "MANHATTAN",
    "BROOKLYN", 
    "QUEENS",
    "BRONX",
    "STATEN ISLAND",
    "Unspecified"
}

LAT_MIN, LAT_MAX = 40.4, 40.9
LON_MIN, LON_MAX = -74.3, -73.7

CRITICAL_COLUMNS = ["unique_key", "created_date", "complaint_type"]


def validate(df):
    # 1. row count check
    if len(df) == 0:
        raise ValueError("DataFrame is empty — nothing to load")

    # 2. critical null checks
    for col in CRITICAL_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Critical column '{col}' is missing from DataFrame")
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Critical column '{col}' has {null_count} nulls")

    # 3. uniqueness check
    duplicate_count = df["unique_key"].duplicated().sum()
    if duplicate_count > 0:
        raise ValueError(f"unique_key has {duplicate_count} duplicates")

    # 4. borough validity check
    if "borough" not in df.columns:
        raise ValueError("Critical column 'borough' is missing from DataFrame")
    invalid_boroughs = df[df["borough"].notna() & ~df["borough"].isin(VALID_BOROUGHS)]
    if len(invalid_boroughs) > 0:
        print(f"WARNING: {len(invalid_boroughs)} rows have invalid borough values")

    # 5. lat/lon range check
    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError("Critical column 'latitude' or 'longitude' is missing from DataFrame")
    out_of_range_lat = df[df["latitude"].notna() & ((df["latitude"] < LAT_MIN) | (df["latitude"] > LAT_MAX))]
    out_of_range_lon = df[df["longitude"].notna() & ((df["longitude"] < LON_MIN) | (df["longitude"] > LON_MAX))]
    if len(out_of_range_lat) > 0:
        print(f"WARNING: {len(out_of_range_lat)} rows have latitude outside NYC bounds")
    if len(out_of_range_lon) > 0:
        print(f"WARNING: {len(out_of_range_lon)} rows have longitude outside NYC bounds")


    return {
        "valid": True,
        "rows": len(df),
        "borough_warnings": len(invalid_boroughs),
        "lat_warnings": len(out_of_range_lat),
        "lon_warnings": len(out_of_range_lon),
    }    


if __name__ == "__main__":
    silver_dir = Path(__file__).parent.parent.parent / "data" / "silver"
    files = sorted(silver_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No silver files found. Run transform.py first.")
    latest = files[-1]
    print(f"Validating: {latest}")

    df = pd.read_parquet(latest)
    result = validate(df)
    print("Validation passed.")