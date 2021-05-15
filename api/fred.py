# import libaries
import requests
import configparser
import psycopg2
import psycopg2.extras
from datetime import date, timedelta

# read config file
config = configparser.ConfigParser()
config.read('../config.ini')

def update_Fred_data():
    
# connect to the PostgreSQL server 
    conn = psycopg2.connect(
            host = config['postgres']['host'],
            dbname = config['postgres']['dbname'],
            user = config['postgres']['user']
        )
    cur = conn.cursor()

    cur.execute("""
        SELECT series_id 
        FROM fred_series;
    """)
    series_list = cur.fetchall()
    print(series_list)

    # run sequence of series through the FRED API to pull data
    start_date = date.today() - timedelta(360)
    start_date = start_date.strftime('%Y-%m-%d')
    sql_data = []
    for series in series_list:

        # get results from FRED query
        base_url = 'https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={key}&observation_start={date}&file_type=json'
        print(base_url.format(series = series[0], key = config['fred']['api_key'], date = start_date))
        r = requests.get(base_url.format(series = series[0], key = config['fred']['api_key'], date = start_date))
        data = r.json()
        observations = data["observations"]

        # convert FRED result JSON into SQL insert string     
        for x in observations:
            if x['value'] != ".":
                sql_data.append((series[0],x['date'],x['value']))

    # update database with FRED data
    psycopg2.extras.execute_values(cur, """
        INSERT INTO fred_series_data (series_id, date, value)
        VALUES %s
        ON CONFLICT (series_id, date) DO NOTHING
    """, sql_data)

    print(cur.rowcount)
    conn.commit()

    cur.close()
    conn.close()