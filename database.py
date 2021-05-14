import psycopg2
import configparser
from api import fred

# read config file
config = configparser.ConfigParser()
config.read('config.ini')

# connect to the PostgreSQL server 
def connect(): 
    return psycopg2.connect(
        host = config['postgres']['host'],
        dbname = config['postgres']['dbname'],
        user = config['postgres']['user']
    )

# connect to database
conn = connect()
cur = conn.cursor()

fred.get_Fred()
