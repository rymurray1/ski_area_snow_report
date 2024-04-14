import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import sqlite3
import datetime


def collect_data():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": [39.1646, 38.9353, 38.684, 37.630768, 39.27484, 39.197, 39.30444, 39.6341, 39.182, 39.1863, 39.6042, 39.4783, 39.479, 38.901, 39.5817, 39.6801, 37.63027, 39.213, 40.485, 37.9167, 39.6061, 39.88332, 37.474759, 48.376, 43.6971, 45.0314, 44.4734, 45.2778, 45.8174, 48.407, 36.596, 44.3659, 43.979282, 45.331845, 40.5777, 40.6, 40.6226, 40.6505, 41.379, 41.2006, 40.5829, 40.615139, 43.6094, 42.9602, 44.5275, 44.135098, 46.9282, 47.4246, 43.818, 43.5934],
        "longitude": [-120.2387, -119.94, -120.068, -119.032631, -120.1206, -120.2357, -120.33583, -105.8715, -106.8564, -106.8182, -106.5165, -106.0723, -106.1613, -106.9672, -105.9437, -105.898, -107.814045, -106.9378, -106.8317, -107.8375, -106.355, -105.773896, -106.793583, -116.6171, -114.3517, -70.3131, -70.8569, -111.4103, -110.8966, -114.3373, -105.4545, -73.9026, -121.688366, -121.664981, -111.624, -111.58333, -111.4851, -111.5045, -111.7807, -111.8614, -111.6556, -111.588917, -72.7968, -72.9204, -72.7839, -72.885962, -121.5045, -121.4164, -110.701, -110.8523],
        "daily": "snowfall_sum",
        "timezone": "America/Denver"
    }

    resorts = ["ALPINE MEADOWS", "HEAVENLY", "KIRKWOOD MOUNTAIN RESORT", "MAMMOTH MOUNTAIN", "NORTHSTAR-AT-TAHOE", "SQUAW VALLEY", "SUGAR BOWL SKI RESORT", "ARAPAHOE BASIN", "ASPEN HIGHLANDS", "ASPEN MOUNTAIN", "BEAVER CREEK", "BRECKENRIDGE SKI RESORT",
                    "COPPER MOUNTAIN", "CRESTED BUTTE MOUNTAIN RESORT","KEYSTONE RESORT","LOVELAND SKI AREA","PURGATORY RESORT","SNOWMASS","STEAMBOAT","TELLURIDE SKI RESORT","VAIL SKI RESORT","WINTER PARK RESORT","WOLF CREEK SKI RESORT","SCHWEITZER MOUNTAIN",
                    "SUN VALLEY","SUGARLOAF MOUNTAIN","SUNDAY RIVER SKI RESORT","BIG SKY RESORT","BRIDGER BOWL","WHITEFISH MOUNTAIN RESORT","TAOS SKI VALLEY","WHITEFACE","MOUNT BACHELOR SKI RESORT","MOUNT HOOD MEADOWS SKI RESORT","ALTA","BRIGHTON RESORT",
                    "DEER VALLEY","PARK CITY MOUNTAIN RESORT","POWDER MOUNTAIN","SNOWBASIN RESORT","SNOWBIRD","SOLITUDE MOUNTAIN RESORT","KILLINGTON RESORT","MOUNT SNOW","STOWE MOUNTAIN RESORT","SUGARBUSH MOUNTAIN SKI RESORT","CRYSTAL MOUNTAIN","SUMMIT AT SNOQUALMIE","GRAND TARGHEE RESORT","JACKSON HOLE MOUNTAIN RESORT"]

    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models

    table = []
    counter = 0

    for response in responses:
        row = {}
        daily = response.Daily()
        daily_snowfall_sum = daily.Variables(0).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}
        daily_data["snowfall_sum"] = daily_snowfall_sum.sum()
        daily_data["resort"] = resorts[counter]

        daily_dataframe = pd.DataFrame(data = daily_data)
        # row = [daily_data["resort"],daily_dataframe["snowfall_sum"]]
        # table.append(row)
        if daily_data["resort"] not in row:
            row["date"] = datetime.date.today()
            row["resort"] = daily_data["resort"]
            row["snowfall_sum"] = daily_data["snowfall_sum"]
            
        table.append(row)
        counter+=1

    # Process daily data. The order of variables needs to be the same as requested.

    # print(table)
    snowfall_dataframe = pd.DataFrame(data = table)
    return(snowfall_dataframe)

df = collect_data()

def update_database(df):
    conn = sqlite3.connect('snow_report2.db')
    cursor = conn.cursor()
    
    # Create table if it does not exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS snow_report (
        forecast_date TEXT,
        resort TEXT,
        snowfall_sum REAL,
        PRIMARY KEY (resort)
    );
    ''')
    
    # Insert or replace daily data
    for _, row in df.iterrows():
        # Correct column names to match those in the dataframe
        cursor.execute('''
        REPLACE INTO snow_report (forecast_date, resort, snowfall_sum)
        VALUES (?, ?, ?);
        ''', (row['date'], row['resort'], row['snowfall_sum']))  # Corrected column names here
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
update_database(df)
def read_data():
    conn = sqlite3.connect('snow_report2.db')
    df = pd.read_sql_query("SELECT * FROM snow_report", conn)
    conn.close()
    return df

df = read_data()
def read_max_data(df):
    df = read_data()
    max_snowfall = df["snowfall_sum"].max()
    max_resort = df["resort"][df["snowfall_sum"].idxmax()]

    return max_snowfall, max_resort



