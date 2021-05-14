# pylint: skip-file
from yahoo_query import update_Yahoo_data
from fred import update_Fred_data
from update_Pocket import update_Pocket_database
from datetime import datetime

# check if today is a weekday before executing any other functions
day_of_week = print(datetime.today().weekday())
if day_of_week < 5:

    # update API data
    update_Yahoo_data()
    update_Fred_data()
    update_Pocket_database()