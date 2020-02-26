import airflow
from airflow.models import Variable
from ast import literal_eval
import datetime


"""
    service_line="opay" --业务线
    country_code = 'NG' --二位国家码
    utc_date_hour = '2020-02-15 23' --调度执行时间(utc)
    gap_hour=0  --小时偏移量
 
"""


def GetLocalTime(service_line,utc_date_hour, country_code, gap_hour):

    service_line_config_file=service_line.lower().strip()+"_time_zone_config"

    #读取配置文件
    config = literal_eval(Variable.get(service_line_config_file))

    #读取国家码对应的[time_zone]
    time_zone = config[country_code]['time_zone']
    time_obj = datetime.datetime.strptime(utc_date_hour, "%Y-%m-%d %H")

    #通过偏移量计算本地时间
    time_obj1 = time_obj + datetime.timedelta(hours=(time_zone+gap_hour))
    re = {
        'date':time_obj1.strftime('%Y-%m-%d'),
        'hour':time_obj1.strftime('%H')
    }
    return re
 
