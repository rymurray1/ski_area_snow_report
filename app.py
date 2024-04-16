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

@app.route('/')
def display():
    df = read_data()
    max_snow_val = read_max_data(df)[0].round(2)
    max_resort = read_max_data(df)[1].title()
    return render_template('index.html', max_resort=max_resort, max_snow_val=max_snow_val)


@app.route('/summer')
def display():
    return render_template('summer.html')