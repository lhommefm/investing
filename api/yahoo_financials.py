# # import libaries
# import configparser
# import psycopg2
# import yfinance as yf

# # read config file
# config = configparser.ConfigParser()
# config.read('../config.ini')

# # connect to the PostgreSQL server 
# conn = psycopg2.connect(
#         host = config['postgres']['host'],
#         dbname = config['postgres']['dbname'],
#         user = config['postgres']['user']
#     )
# cur = conn.cursor()

# # connect to database & pull list of stock series
# cur.execute("""
#     SELECT ticker 
#     FROM stock_list
#     LIMIT 2;
# """)
# ticker_list = cur.fetchall()
# ticker_list = [x[0] for x in ticker_list]
# ticker_list = " ".join(ticker_list)
# print(ticker_list)

# # download yFinance price data for each ticker pulled
# sql_data = []
# data = yf.Tickers(ticker_list)
# info = data.tickers

# # loop through financials data and format for SQL insert
# for ticker in info:
#     p = info[ticker].history()
#     t = info[ticker].info
#     sql_data.append(
#         (p.index.max(), 
#         t.get('symbol'), 
#         t.get('shortName'),
#         t.get('yield'),
#         t.get('dividendRate'),
#         t.get('beta'),                
#         t.get('trailingPE'),
#         t.get('priceToSalesTrailing12Months'),
#         t.get('forwardPE'),
#         t.get('dividendYield'),             
#         t.get('enterpriseToRevenue'),
#         t.get('beta3Year'),
#         t.get('priceToBook'),
#         t.get('pegRatio'),                   
#     ))

# print(sql_data)



# # copy from temp table to correct table, ignoring duplicates
# cur.execute("""
#     INSERT INTO stock_history
#     SELECT *
#     FROM temp_table
#     ON CONFLICT DO NOTHING;
# """)
# conn.commit()

# # close database connection
# cur.close()
# conn.close()