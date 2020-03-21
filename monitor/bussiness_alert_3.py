# coding: utf-8
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import logging
import os
from plugins.DingdingAlert import DingdingAlert
import paramiko
from scp import SCPClient
import time
import datetime, time
import requests
from influxdb import InfluxDBClient
import json
import redis
import random

args = {
    'owner': 'linan',
    'start_date': datetime.datetime(2020, 3, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    # 'email': ['bigdata_dw@opay-inc.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
}

dag = airflow.DAG(
    'bussiness_alert_3',
    schedule_interval="18,28,38,48,58 * * * *",
    concurrency=15,
    default_args=args)

UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# 默认预警地址
defalut_dingding_alert = "https://oapi.dingtalk.com/robot/send?access_token=ce1272d8448e8bd80cd8f2e6eb37ae1be13690013ebaf708517c7ae7162101bd"

#  metrics_name 指标名称，建议使用指标主题范围加业务线或渠道做区分,
#  influx_sql, (字段名称一定要写别名，绑定预警模板名称,在airflow 变量中设置模板)
#  alert_value_name [小于判断比例，大于判断比例],
#  compare_day 对比回推天数,
#  alert_1_level_address_name 一级预警地址,
#  alert_2_level_address_name 二级预警地址,
#  是否关闭预警，预警模板
# alert_mode 判断大于小于规则关系（1 小于，2 大于，3 大于和小于）
metrcis_list = [

    ####### 交易相关指标

    # 短息发送量
    # 60
    (
        'Channel_Message_Send_Chuanglan',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'chuanglan' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 61
    (
        'Channel_Message_Send_Success_Chuanglan',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'chuanglan' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## cm

    # 短息发送量
    # 62
    (
        'Channel_Message_Send_Cm',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'cm' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 63
    (
        'Channel_Message_Send_Success_Cm',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'cm' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## dpb

    # 短息发送量
    # 64
    (
        'Channel_Message_Send_Dpb',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'dpb' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 65
    (
        'Channel_Message_Send_Success_Dpb',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'dpb' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## infobip

    # 短息发送量
    # 66
    (
        'Channel_Message_Send_Infobip',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'infobip' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 67
    (
        'Channel_Message_Send_Success_infobip',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'infobip' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## chuangadvert

    # 短息发送量
    # 68
    (
        'Channel_Message_Send_Chuangadvert',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'chuangadvert' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 69
    (
        'Channel_Message_Send_Success_Chuangadvert',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'chuangadvert' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## infobipws

    # 短息发送量
    # 70
    (
        'Channel_Message_Send_Infobipws',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'infobipws' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 71
    (
        'Channel_Message_Send_Success_Infobipws',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'infobipws' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## infobipvoice

    # 短息发送量
    # 72
    (
        'Channel_Message_Send_Infobipvoice',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'infobipvoice' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 73
    (
        'Channel_Message_Send_Success_Infobipvoice',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'infobipvoice' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## chuangghana

    # 短息发送量
    # 74
    (
        'Channel_Message_Send_Chuangghana',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'chuangghana' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 75
    (
        'Channel_Message_Send_Success_Chuangghana',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'chuangghana' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    ## cmghana

    # 短息发送量
    # 76
    (
        'Channel_Message_Send_Cmghana',
        '''
        SELECT count(distinct("id")) AS "message_send_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE  "delivered_channel" = 'cmghana' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

    # 短息发送成功量
    # 77
    (
        'Channel_Message_Send_Success_Cmghana',
        '''
        SELECT count(distinct("id")) AS "message_send_success_cnt" FROM "OPAY_SMS_MESSAGE_EVENT" WHERE ("status" = 'SUCCESS') AND "delivered_channel" = 'cmghana' AND time > {time} GROUP BY time(10m)
        ''',
        'message_send_success_alert_value',
        7,
        'message_alert_level_1_address',
        'message_alert_level_2_address',
        False,
        3
    ),

]


def get_redis_client():
    redis_client = redis.Redis(host='r-d7o4oicvcs16n22tnu.redis.eu-west-1.rds.aliyuncs.com', port=6379, db=4,
                               decode_responses=True)
    return redis_client


def alert(metrics_name, last_value, compare_value, alert_value, last_seconds, compare_day_ago_second,
          alert_1_level_name,
          alert_2_level_name,
          is_close_alert,
          alert_template_name):
    time = datetime.datetime.fromtimestamp(int(last_seconds)).strftime(DATE_FORMAT)
    compare_time = datetime.datetime.fromtimestamp(int(compare_day_ago_second)).strftime(DATE_FORMAT)

    logging.info(" =========  监控业务线指标名称  : {}  ".format(metrics_name))

    dingding_level_1_alert = None
    alert_template = Variable.get(alert_template_name)
    alert_value_1 = alert_value[0]
    alert_value_2 = alert_value[1]

    redis_client = get_redis_client()

    # 是否手动关闭预警
    if is_close_alert:
        dingding_level_1_alert = DingdingAlert(defalut_dingding_alert)

        dingding_level_1_alert.send(alert_template.format(
            time=time,
            compare_time=compare_time,
            metrics_name=metrics_name,
            last_value=last_value,
            compare_value=compare_value,
            alert_value_1="{}%".format(alert_value_1),
            alert_value_2="{}%".format(alert_value_2))
        )
        logging.info(" =========  进入关闭预警流程 ....... ")

    else:
        logging.info(" =========  进入 LEVEL 1  预警 .......")

        dingding_level_1_alert = DingdingAlert(Variable.get(alert_1_level_name))
        dingding_level_1_alert.send(alert_template.format(
            time=time,
            compare_time=compare_time,
            metrics_name=metrics_name,
            last_value=last_value,
            compare_value=compare_value,
            alert_value_1="{}%".format(alert_value_1),
            alert_value_2="{}%".format(alert_value_2))
        )

        logging.info(" =========  LEVEL 1 预警成功 ....... ")

        key = "{}_{}".format(metrics_name, alert_template_name)

        alert_times = redis_client.get(key)

        logging.info(" =========  预警记录次数 : {}  ".format(alert_times))

        if alert_times == None:
            alert_times = 1
        else:
            alert_times = int(alert_times)

        if alert_times >= 4:
            logging.info(" =========  进入 LEVEL 2  预警 .......")
            dingding_level_2_alert = DingdingAlert(Variable.get(alert_2_level_name))
            dingding_level_2_alert.send(alert_template.format(
                time=time,
                compare_time=compare_time,
                metrics_name=metrics_name,
                last_value=last_value,
                compare_value=compare_value,
                alert_value_1="{}%".format(alert_value_1),
                alert_value_2="{}%".format(alert_value_2))
            )
            logging.info(" =========  LEVEL 2 预警成功 ....... ")

        alert_times += 1
        redis_client.set(key, alert_times)

    redis_client.close()


# 清除之前所有记录预警次数
def clear_error_times(metrics_name, alert_template_name):
    redis_client = get_redis_client()

    key = "{}_{}".format(metrics_name, alert_template_name)
    redis_client.set(key, 0)
    logging.info(" =========  未发现异常，清除预警累计次数  {}  ..... ".format(key))

    redis_client.close()


# 判断小于
def handle_mode_1(metrics_name, last_value, compare_value, alert_value_1, alert_value_2, last_time,
                  compare_day_ago_second,
                  alert_1_level_name,
                  alert_2_level_name,
                  is_close_alert,
                  template_name
                  ):
    if last_value < int(compare_value * alert_value_1):
        alert_value_1 = int(alert_value_1 * 100)
        alert(metrics_name, last_value, compare_value, [alert_value_1, ''], last_time,
              compare_day_ago_second,
              alert_1_level_name,
              alert_2_level_name, is_close_alert, template_name)
    else:
        clear_error_times(metrics_name, template_name)


# 判断大于
def handle_mode_2(metrics_name, last_value, compare_value, alert_value_1, alert_value_2, last_time,
                  compare_day_ago_second,
                  alert_1_level_name,
                  alert_2_level_name,
                  is_close_alert,
                  template_name
                  ):
    if last_value > int(compare_value * alert_value_2):
        alert_value_2 = int(alert_value_2 * 100)
        alert(metrics_name, last_value, compare_value, ['', alert_value_2], last_time,
              compare_day_ago_second,
              alert_1_level_name,
              alert_2_level_name, is_close_alert, template_name)
    else:
        clear_error_times(metrics_name, template_name)


# 判断大于和小于情况
def handle_mode_3(metrics_name, last_value, compare_value, alert_value_1, alert_value_2, last_time,
                  compare_day_ago_second,
                  alert_1_level_name,
                  alert_2_level_name,
                  is_close_alert,
                  template_name
                  ):
    if last_value < int(compare_value * alert_value_1) or last_value > int(compare_value * alert_value_2):

        alert_value_1 = int(alert_value_1 * 100)
        alert_value_2 = int(alert_value_2 * 100)

        alert(metrics_name, last_value, compare_value, [alert_value_1, alert_value_2], last_time,
              compare_day_ago_second,
              alert_1_level_name,
              alert_2_level_name, is_close_alert, template_name)
    else:
        clear_error_times(metrics_name, template_name)


def monitor_task(ds, metrics_name, influx_db_query_sql, alert_value_name, compare_day, alert_1_level_name,
                 alert_2_level_name, is_close_alert, mode, **kwargs):
    last_time = 0
    data_map = dict()

    ## 增加随机数延迟

    # sleep = random.randint(10, 300)
    #
    # time.sleep(sleep)
    # logging.info(" =========  随机时间等待 : {} s ".format(sleep))

    influx_client = InfluxDBClient('10.52.5.233', 8086, 'bigdata', 'opay321', 'serverDB')

    date_time = datetime.datetime.strptime(ds, '%Y-%m-%d')
    time_condition = (date_time - datetime.timedelta(days=(compare_day + 1)))
    time_condition = int(time.mktime(time_condition.timetuple()))
    time_condition = "{}000000000".format(time_condition)

    logging.info(" =========  time_condition : {}".format(time_condition))

    alert_values = eval(Variable.get(alert_value_name))
    alert_value_1 = alert_values[0]
    alert_value_2 = alert_values[1]

    query_sql = influx_db_query_sql.format(time=time_condition)
    logging.info(" =========  query sql : {} ".format(query_sql))

    res = influx_client.query(query_sql)
    raw = res.raw
    series = raw.get('series')

    if series is None:
        logging.info(" =========  No data  ".format(str(raw)))
        return

    values = series[0]['values']
    columns = series[0]['columns']

    for i in range(len(values)):
        line = values[i]
        timestamp = line[0]
        utcTime = datetime.datetime.strptime(timestamp, UTC_FORMAT)
        timesecond = int(time.mktime(utcTime.timetuple()))
        data_map[timesecond] = line
        last_time = timesecond

        ## 获取倒数第二最新时间
        if i == len(values) - 2:
            break

    date = datetime.datetime.utcfromtimestamp(last_time)
    compare_day_ago = date - datetime.timedelta(days=compare_day)

    compare_day_ago_second = int(time.mktime(compare_day_ago.timetuple()))

    last_obj = data_map[last_time]
    compare_obj = data_map[compare_day_ago_second]

    for i in range(len(last_obj)):
        if i == 0:
            continue

        last_metrcis_value = None
        compare_metrcis_value = None
        if last_obj[i] is None:
            last_metrcis_value = 0
        else:
            last_metrcis_value = int(last_obj[i])

        if compare_obj[i] is None:
            compare_metrcis_value = 0
        else:
            compare_metrcis_value = int(compare_obj[i])

        logging.info(" =========  最新数据  时间 ：{}  , 指标值： {}  ".format(
            datetime.datetime.fromtimestamp(int(last_time)).strftime(DATE_FORMAT), last_metrcis_value))
        logging.info(" =========  对比日数据  时间 ：{}  , 指标值： {}  ".format(
            datetime.datetime.fromtimestamp(int(compare_day_ago_second)).strftime(DATE_FORMAT), compare_metrcis_value))
        logging.info(" =========  预警阈值比例值  ： {}   {}  ".format(alert_value_1, alert_value_2))
        logging.info(" =========  处理 mode  ： {}    ".format(mode))

        if mode == 1:
            handle_mode_1(metrics_name, last_metrcis_value, compare_metrcis_value, alert_value_1, alert_value_2,
                          last_time,
                          compare_day_ago_second,
                          alert_1_level_name,
                          alert_2_level_name, is_close_alert, columns[i])
        elif mode == 2:
            handle_mode_2(metrics_name, last_metrcis_value, compare_metrcis_value, alert_value_1, alert_value_2,
                          last_time,
                          compare_day_ago_second,
                          alert_1_level_name,
                          alert_2_level_name, is_close_alert, columns[i])
        elif mode == 3:
            handle_mode_3(metrics_name, last_metrcis_value, compare_metrcis_value, alert_value_1, alert_value_2,
                          last_time,
                          compare_day_ago_second,
                          alert_1_level_name,
                          alert_2_level_name, is_close_alert, columns[i])

    influx_client.close()


for metrics_name, influx_db_query_sql, alert_value_name, compare_day, alert_1_level_name, alert_2_level_name, is_close_alert, mode in metrcis_list:
    monitor = PythonOperator(
        task_id='monitor_task_{}'.format(metrics_name),
        python_callable=monitor_task,
        provide_context=True,
        op_kwargs={
            'metrics_name': metrics_name,
            'influx_db_query_sql': influx_db_query_sql,
            'alert_value_name': alert_value_name,
            'compare_day': compare_day,
            'alert_1_level_name': alert_1_level_name,
            'alert_2_level_name': alert_2_level_name,
            'is_close_alert': is_close_alert,
            'mode': mode
        },
        dag=dag
    )
