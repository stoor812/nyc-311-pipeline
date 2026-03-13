import requests   # makes HTTP calls to the API (like fetch() in JS)
import pandas as pd  # DataFrame — your main data structure
from datetime import datetime  # for timestamping the bronze filename
from pathlib import Path  # for building file paths cleanly


API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
offset = 0
limit = 1000
data = []
while True:
    response = requests.get(API_URL, params={"$offset": offset, "$limit": limit})



    
dataFrame = pd.DataFrame(data)
d