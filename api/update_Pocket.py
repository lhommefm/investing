import psycopg2
import psycopg2.extras
import configparser

def update_Pocket_database():
    
    # read config file
    config = configparser.ConfigParser()
    config.read('../config.ini')

    ############################################
    # connect to the Investing PostgreSQL server 
    conn1 = psycopg2.connect(
            host = config['postgres']['host'],
            dbname = config['postgres']['dbname'],
            user = config['postgres']['user']
        )
    cur1 = conn1.cursor()

    # download fred data
    cur1.execute("""
        SELECT "series_id", "date", "value" 
        FROM fred_series_data 
        WHERE "date" > CURRENT_TIMESTAMP - INTERVAL '45 DAY';
    """)
    fred_data = cur1.fetchall()


    # download stock history
    cur1.execute("""
        SELECT ticker, "date", adjclose 
        FROM stock_history
        WHERE date > CURRENT_TIMESTAMP - INTERVAL '7 DAY';
    """)
    stock_history = cur1.fetchall()

    # download stock financials
    cur1.execute("""
        SELECT ticker, name, yield, pe, pb, beta, date 
        FROM stock_financials
        WHERE date > CURRENT_TIMESTAMP - INTERVAL '7 DAY';
    """)
    stock_financials = cur1.fetchall()

    #####################################
    # close Investing database connection
    cur1.close()
    conn1.close()

    print(f'fred_data ==> {fred_data[0]}')
    print(f'stock_history ==> {stock_history[0]}')
    print(f'stock_financials ==> {stock_financials[0]}')

    #########################################
    # connect to the Heroku PostgreSQL server 
    conn2 = psycopg2.connect(
            host = config['heroku']['host'],
            dbname = config['heroku']['dbname'],
            user = config['heroku']['user'],
            password = config['heroku']['password']
        )
    cur2 = conn2.cursor()

    # upload fred data
    psycopg2.extras.execute_values(cur2, """
        INSERT INTO fred_series_data (series_id, date, value)
        VALUES %s
        ON CONFLICT (series_id, date) DO NOTHING
    """, fred_data)

    # upload stock history
    psycopg2.extras.execute_values(cur2, """
        INSERT INTO stock_history (ticker, date, price)
        VALUES %s
        ON CONFLICT (ticker, date) DO NOTHING
    """, stock_history)

    # upload stock financials
    psycopg2.extras.execute_values(cur2, """
        INSERT INTO stock_detail_data (ticker, short_name, yield, price_earnings, price_book, beta, date)
        VALUES %s
        ON CONFLICT (ticker, date) DO NOTHING
    """, stock_financials)

    conn2.commit()

    ##################################
    # close Heroku database connection
    cur2.close()
    conn2.close()
    return(True)