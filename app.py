from flask import Flask, render_template
from api_analysis import read_max_data, read_data

app = Flask(__name__)

@app.route('/')
def display():
    df = read_data()
    max_snow_val = read_max_data(df)["snowfall_sum"].round(2)
    max_resort = read_max_data(df)["resort"]
    return render_template('index.html', max_resort=max_resort, max_snow_val=max_snow_val)
