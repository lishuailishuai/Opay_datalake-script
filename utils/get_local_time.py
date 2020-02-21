import airflow
from airflow.models import Variable
from ast import literal_eval
import datetime

def GetLocalTime(utc_date_hour, country_code, gap_hour):
    print(utc_date_hour)
    config = literal_eval(Variable.get("utc_locale_time_config"))
    time_zone = config[country_code]['time_zone']
    time_obj = datetime.datetime.strptime(utc_date_hour, "%Y-%m-%d %H")
    time_obj1 = time_obj + datetime.timedelta(hours=(time_zone+gap_hour))
    re = {
        'date':time_obj1.strftime('%Y-%m-%d'),
        'hour':time_obj1.strftime('%H')
    }
    return re
 
# country_code = 'NG'
# utc_date_hour = '2020-02-15 23'
# gap_hour=0
# d = GetLocalTime(utc_date_hour, country_code, gap_hour)
# print(d)