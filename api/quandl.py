# import libaries
import requests
import configparser
import psycopg2
import psycopg2.extras
from datetime import date, timedelta

def update_Quandl_data():

    # read config file
    config = configparser.ConfigParser()
    config.read('../config.ini')
        
    # connect to the PostgreSQL server 
    conn = psycopg2.connect(
            host = config['postgres']['host'],
            dbname = config['postgres']['dbname'],
            user = config['postgres']['user']
        )
    cur = conn.cursor()

    cur.execute("""
        SELECT series_id 
        FROM quandl_series;
    """)
    series_list = cur.fetchall()
    print(series_list)

    # run sequence of series through the Quandl API to pull data
    sql_data = []
    for series in series_list:

        # get results from FRED query
        base_url = 'https://www.quandl.com/api/v3/datasets/{series}/data.json?&api_key={key}&start_date={date}'
        print(base_url.format(series = series[0], key = config['quandl']['api_key'], date = date.today() - timedelta(60)))
        r = requests.get(base_url.format(series = series[0], key = config['quandl']['api_key'], date = date.today() - timedelta(60)))
        data = r.json()
        data = data['dataset_data']['data']

        # convert FRED result JSON into SQL insert string     
        for x in data:
            sql_data.append((series[0],x[0],x[1]))
    print(sql_data[0])

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