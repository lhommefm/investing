# import libaries
import configparser
import psycopg2
import yfinance as yf
import pandas as pd
from io import StringIO

# read config file
config = configparser.ConfigParser()
config.read('../config.ini')

##################################
# connect to the PostgreSQL server 
conn = psycopg2.connect(
        host = config['postgres']['host'],
        dbname = config['postgres']['dbname'],
        user = config['postgres']['user']
    )
cur = conn.cursor()

#################################################
# connect to database & pull list of stock series
cur.execute("""
    SELECT ticker 
    FROM stock_list;
""")
ticker_list = cur.fetchall()
ticker_list = [x[0] for x in ticker_list]
print(ticker_list)

#####################################################
# download yFinance price data for each ticker pulled
df_list = list()
for ticker in ticker_list:
    data = yf.download(ticker, group_by="Ticker", period="max")
    data['ticker'] = ticker
    df_list.append(data)
print('Yahoo Price data downloaded')

################################################
# combine all dataframes into a single dataframe
df = pd.concat(df_list)

# use StringIO to create a file like object from strings
buffer = StringIO()

# write dataframe as a CSV to the the temp object 
df.to_csv(buffer, header=False)
buffer.seek(0)

# create a temp database table to copy into
cur.execute("""
    CREATE TEMP TABLE temp_table
    (LIKE stock_history INCLUDING DEFAULTS)
    ON COMMIT DROP;
""")
cur.copy_from(buffer, 'temp_table', sep=",")

# copy from temp table to correct table, ignoring duplicates
cur.execute("""
    INSERT INTO stock_history
    SELECT *
    FROM temp_table
    ON CONFLICT DO NOTHING;
""")
conn.commit()
print('Yahoo Price data uploaded to database')

###########################
# close database connection
cur.close()
conn.close()