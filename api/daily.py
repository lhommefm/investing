# pylint: skip-file
from yahoo_query import update_Yahoo_data
from fred import update_Fred_data
from quandl import update_Quandl_data
from update_Pocket import update_Pocket_database
from datetime import datetime

# check if today is a weekday before executing any other functions
day_of_week = datetime.today().weekday()
if day_of_week < 5:

    # update API data every day
    update_Yahoo_data()
    update_Quandl_data()

    # update API data every other day
    if day_of_week in [0,2,4]: 
        update_Fred_data()
    
    # update the Pocket database
    update_Pocket_database()