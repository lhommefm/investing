# import libaries
import configparser
import psycopg2
import psycopg2.extras
from yahooquery import Ticker
import pandas as pd
from io import StringIO

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

#################################################
# connect to database & pull list of stock series
cur.execute("""
    SELECT ticker 
    FROM stock_list;
""")
ticker_list = cur.fetchall()
ticker_list = [x[0] for x in ticker_list]
print(ticker_list)

######################################################
# download yFinance price data for each ticker pulled
data = Ticker(ticker_list)
f = data.fund_equity_holdings
h = data.history()
k = data.key_stats
s = data.summary_detail

########################################################
# loop through financials data and format for SQL insert
sql_data = []
for ticker in f:
    print(ticker)
    if isinstance(f[ticker], str):
        sql_data.append((
            f'{h.index.max()[1]}', 
            ticker,
            s[ticker].get('trailingPE'),
            k[ticker].get('priceToBook'),
            s[ticker].get('priceToSalesTrailing12Months'),
            None,
            s[ticker].get('beta'),          
            s[ticker].get('dividendYield')                   
        ))
    else:
        sql_data.append((
            f'{h.index.max()[1]}', 
            ticker,
            None if f[ticker].get('priceToEarnings') == 0 else f[ticker].get('priceToEarnings'),
            None if f[ticker].get('priceToBook') == 0 else f[ticker].get('priceToBook'),
            None if f[ticker].get('priceToSales') == 0 else f[ticker].get('priceToSales'),
            None if f[ticker].get('priceToCashflow') == 0 else f[ticker].get('priceToCashflow'),
            k[ticker].get('beta3Year'),
            k[ticker].get('yield')
        ))          
print(sql_data)

# update database with stock financials
psycopg2.extras.execute_values(cur, """
    INSERT INTO stock_financials (date, ticker, pe, pb, ps, pcf, beta, yield)
    VALUES %s
    ON CONFLICT (date, ticker) DO NOTHING
""", sql_data)

print(cur.rowcount)
conn.commit()

##########################################
# format price history as pandas dataframe
df_list = list()
df_list.append(data.history())
df = pd.concat(df_list)
del df['dividends']
df = df[['open','high','low','close','adjclose','volume']]

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
cur.copy_from(buffer, 'temp_table', sep=",", 
    columns=['ticker','date','open','high','low','close','adjclose','volume']
)

# copy from temp table to correct table, ignoring duplicates
cur.execute("""
    INSERT INTO stock_history (ticker, date, open, high, low, close, adjclose, volume)
    SELECT ticker, date, open, high, low, close, adjclose, volume
    FROM temp_table
    ON CONFLICT DO NOTHING;
""")
print(cur.rowcount)
conn.commit()

###########################
# close database connection
cur.close()
conn.close()