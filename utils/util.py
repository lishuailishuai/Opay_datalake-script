# -*- coding: utf-8 -*-
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta

repair_table_query = '''
MSCK REPAIR TABLE %s
'''
work_times = 15 * 3600
driver_online_time_key = "online_time:time:2:{driver_id}:{dt}"
# dt format YYYYmmDD


def mapper(x):
    if x is None:
        x = 0
    return x


def raw_data_mapper(x):
    res = 0
    try:
        res = int(x)
    except:
        pass
    return res


def query_repair_table(sql):
    cursor = get_hive_cursor()
    cursor.execute(sql)
    cursor.close()


def query_hive_data(sql):
    cursor = get_hive_cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def n_days_ago(n_time, days):
    now_time = datetime.strptime(n_time, '%Y-%m-%d')
    delta = timedelta(days=days)
    n_days = now_time - delta
    return n_days.strftime("%Y-%m-%d")


def double_digit(x):
    if x < 10:
        return "0" + str(x)
    return str(x)


def time_transfer(seconds):
    hour = seconds // 3600
    minute = (seconds % 3600) // 60
    sec = seconds % 60
    res = ""
    if hour > 0:
        res = "{hour}h:{min}m:{sec}s".format(hour=str(hour),
                                                       min=str(minute), sec=double_digit(sec))
    elif minute > 0:
        res = "{min}m:{sec}s".format(min=str(minute), sec=double_digit(sec))
    elif sec > 0:
        res = "{sec}s".format(sec=str(sec))
    return res
