import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, Table, Column, String, Float, DateTime, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import os
import numpy as np

load_dotenv()

metadata = MetaData()

service_requests = Table(
    "service_requests",
    metadata,
    Column("unique_key", String, primary_key=True),
    Column("created_date", DateTime),
    Column("closed_date", DateTime),
    Column("agency", String),
    Column("agency_name", String),
    Column("complaint_type", String),
    Column("descriptor", String),
    Column("incident_zip", String),
    Column("address_type", String),
    Column("city", String),
    Column("status", String),
    Column("resolution_description", String),
    Column("resolution_action_updated_date", DateTime),
    Column("community_board", String),
    Column("council_district", String),
    Column("police_precinct", String),
    Column("borough", String),
    Column("open_data_channel_type", String),
    Column("park_facility_name", String),
    Column("park_borough", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("x_coordinate_state_plane", Float),
    Column("y_coordinate_state_plane", Float),
    Column("incident_address", String),
    Column("street_name", String),
    Column("cross_street_1", String),
    Column("cross_street_2", String),
    Column("intersection_street_1", String),
    Column("intersection_street_2", String),
    Column("bbl", String),
    Column("landmark", String),
    Column("descriptor_2", String),
    Column("location_type", String),
)


def get_engine():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    if not all([host, port, name, user, password]):
        raise ValueError("Missing database credentials in .env file")
    
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)

def create_table(engine):
    metadata.create_all(engine)
    print("Table created or already exists.")


CHUNK_SIZE = 1000

def upsert(df, engine):
    # only keep columns that exist in both the df and the table definition
    table_columns = {col.name for col in service_requests.columns}
    df = df[[col for col in df.columns if col in table_columns]]

    # replace NaT/NaN with None so PostgreSQL handles nulls cleanly
    df = df.replace({np.nan: None})

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            stmt = insert(service_requests).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["unique_key"],
                set_={col: stmt.excluded[col] for col in df.columns if col != "unique_key"}
            )
            conn.execute(stmt)
    print(f"Upserted {len(records)} records.")

if __name__ == "__main__":
    silver_dir = Path(__file__).parent.parent.parent / "data" / "silver"
    files = sorted(silver_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No silver files found. Run transform.py first.")
    latest = files[-1]
    print(f"Loading: {latest}")

    df = pd.read_parquet(latest)
    engine = get_engine()
    create_table(engine)
    upsert(df, engine)
    print("Load complete.")