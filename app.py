from flask import Flask
from api_analysis import read_max_data, read_data
app = Flask(__name__)

@app.route('/')
def hello_world():
    df = read_data()
    max_snow_val = str(read_max_data(df)["snowfall_sum"].round(2))
    max_resort = str(read_max_data(df)["resort"])
    output = display_val = f"The best resort to ski at this week is <strong>{max_resort}</strong> with <strong>{max_snow_val}</strong> inches of anticipated snow over the next 7 days."
    return output
