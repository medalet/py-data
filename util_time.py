import calendar as cal
import datetime

def get_month_timestamp(year,mon):
    d = cal.monthrange(year,mon)
    first_timestamp = datetime.datetime(year,mon,1,0,0,0).timestamp()
    last_timestamp  = datetime.datetime(year,mon,d[1],23,59,59).timestamp()
    return first_timestamp,last_timestamp
