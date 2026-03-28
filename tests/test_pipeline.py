import pandas as pd
import pytest
from src.pipeline.transform import transform_data
from src.pipeline.validate import validate


def make_sample_df():
    return pd.DataFrame([{
        "unique_key": "12345",
        "created_date": "2024-01-01T00:00:00.000",
        "closed_date": None,
        "complaint_type": "Noise",
        "borough": "BROOKLYN",
        "latitude": "40.65",
        "longitude": "-73.95",
        "x_coordinate_state_plane": "987000.0",
        "y_coordinate_state_plane": "182000.0",
        "agency": "NYPD",
        "agency_name": "New York City Police Department",
        "status": "Open",
        "incident_zip": "11201",
        "descriptor": "Loud Music",
        "resolution_description": None,
        "resolution_action_updated_date": None,
        "community_board": "01 BROOKLYN",
        "council_district": "33",
        "police_precinct": "Precinct 84",
        "open_data_channel_type": "ONLINE",
        "park_facility_name": "Unspecified",
        "park_borough": "BROOKLYN",
        "location_type": None,
        "incident_address": "123 MAIN ST",
        "street_name": "MAIN ST",
        "cross_street_1": None,
        "cross_street_2": None,
        "intersection_street_1": None,
        "intersection_street_2": None,
        "bbl": None,
        "landmark": None,
        "descriptor_2": None,
        "address_type": "ADDRESS",
        "city": "BROOKLYN",
        "facility_type": None,
        "location": None,
        "due_date": None,
        "bridge_highway_name": None,
        "bridge_highway_direction": None,
        "road_ramp": None,
        "bridge_highway_segment": None,
        "taxi_pick_up_location": None,
    }])


# --- transform tests ---

def test_date_columns_are_datetime():
    df = make_sample_df()
    df = transform_data(df)
    assert pd.api.types.is_datetime64_any_dtype(df["created_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["closed_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["resolution_action_updated_date"])


def test_float_columns_are_float():
    df = make_sample_df()
    df = transform_data(df)
    assert pd.api.types.is_float_dtype(df["latitude"])
    assert pd.api.types.is_float_dtype(df["longitude"])
    assert pd.api.types.is_float_dtype(df["x_coordinate_state_plane"])
    assert pd.api.types.is_float_dtype(df["y_coordinate_state_plane"])


def test_drop_columns_are_removed():
    df = make_sample_df()
    df = transform_data(df)
    dropped = [
        "due_date",
        "bridge_highway_name",
        "bridge_highway_direction",
        "bridge_highway_segment",
        "road_ramp",
        "taxi_pick_up_location",
        "location",
    ]
    for col in dropped:
        assert col not in df.columns


# --- validate tests ---

def test_validate_passes_on_good_data():
    df = make_sample_df()
    df = transform_data(df)
    result = validate(df)
    assert result["rows"] == 1


def test_validate_raises_on_empty_df():
    df = pd.DataFrame()
    with pytest.raises(ValueError):
        validate(df)


def test_validate_raises_on_null_unique_key():
    df = make_sample_df()
    df = transform_data(df)
    df["unique_key"] = None
    with pytest.raises(ValueError):
        validate(df)


def test_validate_raises_on_duplicate_unique_key():
    df = make_sample_df()
    df = transform_data(df)
    df = pd.concat([df, df], ignore_index=True)
    with pytest.raises(ValueError):
        validate(df)


def test_validate_warns_on_out_of_range_coordinates():
    df = make_sample_df()
    df = transform_data(df)
    df["latitude"] = 999.0
    result = validate(df)
    assert result["lat_warnings"] == 1