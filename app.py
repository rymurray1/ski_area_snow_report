from flask import Flask, render_template
from apscheduler.schedulers.background import BackgroundScheduler
from api_analysis import collect_data, update_database, read_max_data, read_data

app = Flask(__name__)

def scheduled_task():
    print("Updating database with new data...")
    df = collect_data()  # Collect data from the API
    update_database(df)  # Update the database with the new data
    print("Database updated.")

# Schedule the 'scheduled_task' to run once a day
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scheduled_task, 'interval', hours=6)
scheduler.start()

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/')
def display():
    df = read_data()
    max_dict = read_max_data(df)
    snowfall = max_dict["max_snowfall"].round(2)
    resort = max_dict["max_resort"].title()
    max_temp = max_dict["max_temperature_2m_max"].round(2)
    min_temp = max_dict["max_temperature_2m_min"].round(2)
    max_wind = max_dict["max_wind_speed_10m_max"].round(2)
    sunshine_hours = max_dict["max_sunshine_duration"].round(2)
    daylight_hours = max_dict["max_daylight_duration"].round(2)
    percent_sunny = max_dict["max_avg_percent_sunny"].round(2)
  
    return render_template('index.html', resort=resort, snowfall=snowfall, max_temp=max_temp,min_temp=min_temp,max_wind=max_wind,sunshine_hours=sunshine_hours, daylight_hours=daylight_hours, percent_sunny= percent_sunny)

