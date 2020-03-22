# coding: utf-8
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import logging
import os
from plugins.DingdingAlert import DingdingAlert
import paramiko
import time
import datetime, time
import requests
from influxdb import InfluxDBClient
import json
import redis
import random

args = {
    'owner': 'linan',
    'start_date': datetime.datetime(2020, 3, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    # 'email': ['bigdata_dw@opay-inc.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
}

dag = airflow.DAG(
    'bussiness_alert',
    schedule_interval="*/10 * * * *",
    concurrency=20,
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
    ## Airtime
    # 1
    (
        'Trade_Airtime',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'airtime_topup_record' AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 2
    (
        'Trade_Airtime_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'airtime_topup_record' AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Betting
    # 3
    (
        'Trade_Betting',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'betting_topup_record' AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 5
    (
        'Trade_Betting_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'betting_topup_record'  AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Electricity
    # 6
    (
        'Trade_Electricity',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'electricity_topup_record'  AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 7
    (
        'Trade_Electricity_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'electricity_topup_record' AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## TV
    # 8
    (
        'Trade_TV',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'tv_topup_record' AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 9
    (
        'Trade_TV_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'tv_topup_record'  AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Mobiledata
    # 10
    (
        'Trade_Mobiledata',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'mobiledata_topup_record'   AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 11
    (
        'Trade_Mobiledata_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'mobiledata_topup_record' AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Cash_In
    # 12
    (
        'Trade_Cash_In',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'cash_in_record' AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 13
    (
        'Trade_Cash_In_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'cash_in_record' AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Cash_Out
    # 14
    (
        'Trade_Cash_Out',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'cash_out_record' AND "__op" = 'c') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 15
    (
        'Trade_Cash_Out_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'cash_out_record' AND "order_status" = 'SUCCESS') and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## ACTransfer
    # 16
    (
        'Trade_ACTransfer',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_transfer_card_record' or "__source_table" = 'merchant_transfer_card_record') AND "__op" = 'c' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 17
    (
        'Trade_ACTransfer_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_transfer_card_record' or "__source_table" = 'merchant_transfer_card_record') AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Pos
    # 18
    (
        'Trade_Pos',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_pos_transaction_record' or "__source_table" = 'merchant_pos_transaction_record') AND "__op" = 'c' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 19
    (
        'Trade_Pos_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_pos_transaction_record' or "__source_table" = 'merchant_pos_transaction_record')  AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## TopupWithCard
    # 20
    (
        'Trade_TopupWithCard',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_topup_record' or "__source_table" = 'merchant_topup_record') AND "__op" = 'c' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 21
    (
        'Trade_TopupWithCard_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_topup_record' or "__source_table" = 'merchant_topup_record') AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Receivemoney
    # 22
    (
        'Trade_Receivemoney',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_receive_money_record' or "__source_table" = 'merchant_receive_money_record') AND "__op" = 'c' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 23
    (
        'Trade_Receivemoney_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_receive_money_record' or "__source_table" = 'merchant_receive_money_record') AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## TakeRide
    # 24
    (
        'Trade_TakeRide',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "__op" = 'c') and "merchant_id" = '256619082800116' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 25
    (
        'Trade_TakeRide_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt"  FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record') and "merchant_id" = '256619082800116' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## OrideSalary
    # 26
    (
        'Trade_OrideSalary',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record'  AND "__op" = 'c') and "merchant_id" = '256619082800116' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 27
    (
        'Trade_OrideSalary_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record') and "merchant_id" = '256619082800116' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Subscribe
    # 28
    (
        'Trade_Subscribe',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record'  AND "__op" = 'c') and "merchant_id" = '256619082801043' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 29
    (
        'Trade_Subscribe_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record') and "merchant_id" = '256619082801043' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Redeem
    # 30
    (
        'Trade_Redeem',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record' AND "__op" = 'c') and "merchant_id" = '256619082801043' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 31
    (
        'Trade_Redeem_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record') and "merchant_id" = '256619082801043' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Repayment
    # 32
    (
        'Trade_Repayment',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "__op" = 'c') and "merchant_id" = '256619082800418' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 33
    (
        'Trade_Repayment_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record') and "merchant_id" = '256619082800418' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## Loan
    # 34
    (
        'Trade_Loan',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record' AND "__op" = 'c') and "merchant_id" = '256619082800418' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 35
    (
        'Trade_Loan_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record') and "merchant_id" = '256619082800418' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## TakeOut
    # 36
    (
        'Trade_TakeOut',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "__op" = 'c') and "merchant_id" = '256619082800041' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 37
    (
        'Trade_TakeOut_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record') and "merchant_id" = '256619082800041' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## FoodCashback
    # 38
    (
        'Trade_FoodCashback',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record' AND "__op" = 'c') and "merchant_id" = '256619082800041' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 39
    (
        'Trade_FoodCashback_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record') and "merchant_id" = '256619082800041' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## QRCode
    # 40
    (
        'Trade_QRCode',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "__op" = 'c') and "merchant_id" = '256619111336006' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 41
    (
        'Trade_QRCode_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record') and "merchant_id" = '256619111336006' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## QRCashback
    # 42
    (
        'Trade_QRCashback',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record' AND "__op" = 'c') and "merchant_id" = '256619102116029' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 43
    (
        'Trade_QRCashback_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_transfer_user_record') and "merchant_id" = '256619102116029' AND "order_status" = 'SUCCESS' and time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## AATransfer
    # 44
    (
        'Trade_AATransfer',
        ''' SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_transfer_user_record' OR "__source_table" = 'merchant_transfer_user_record') AND "__op" = 'c' AND "merchant_id" != '256619082800116' AND "merchant_id" != '256619082801043' AND "merchant_id" != '256619082800418' AND "merchant_id" != '256619082800041' AND "merchant_id" != '256619102116029' AND time > {time}  GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 45
    (
        'Trade_AATransfer_Success',
        ''' SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'user_transfer_user_record' OR "__source_table" = 'merchant_transfer_user_record') AND ("order_status" = 'SUCCESS' or "transfer_status" = 'CONFIRM_S'  or "transfer_status" = 'TRANSFER_S') AND "merchant_id" != '256619082800116' AND "merchant_id" != '256619082801043' AND "merchant_id" != '256619082800418' AND "merchant_id" != '256619082800041' AND "merchant_id" != '256619102116029' AND time > {time}  GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## MAcquiring
    # 46
    (
        'Trade_MAcquiring',
        '''SELECT count(distinct("order_no")) AS "trade_cnt",count(distinct("user_id")) AS "trade_user_cnt" ,sum("amount") AS "trade_amount" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "__op" = 'c'  AND "merchant_id" != '256619082800116' AND "merchant_id" != '256619082801043' AND "merchant_id" != '256619082800418' AND "merchant_id" != '256619082800041' ) AND time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    # 47
    (
        'Trade_MAcquiring_Success',
        '''SELECT count(distinct("order_no")) AS "trade_success_cnt" FROM "OPAY_TRANSACTION_OP_EVENT" WHERE ("__source_table" = 'merchant_acquiring_record' AND "merchant_id" != '256619082800116' AND "merchant_id" != '256619082801043' AND "merchant_id" != '256619082800418' AND "merchant_id" != '256619082800041' ) AND "order_status" = 'SUCCESS' AND time > {time} GROUP BY time(10m) ''',
        'trade_alert_value',
        7,
        'trade_alert_level_1_address',
        'trade_alert_level_2_address',
        False,
        3
    ),

    ## 渠道 INTERSWITCH

    # 通道交易数
    # 48
    (
        'Trade_Channel_INTERSWITCH',
        '''SELECT count(distinct("id")) AS "channel_trade_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'INTERSWITCH' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    # 通道系统交易成功数
    # 49
    (
        'Trade_Channel_System_Success_INTERSWITCH',
        '''SELECT count(distinct("id")) AS "trade_channel_system_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'INTERSWITCH' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} 
        AND (
        "transaction_status" = 'SUCCESS' or
        "bank_response_message" = 'Transaction not Found' or
        "bank_response_message" = 'Card Expiry is not in correct format' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Amount greater than daily transaction limit Card has expired Exceeds Withdrawal Limit Expired Card Lost Card or Pick-Up Not sufficient funds Error occurred while processing your request... Please contact merchant Insufficient Funds Token Not Generated. Customer Not Registered on Token Platform Transaction not Permitted to Cardholder Token Authorization Not Successful. Incorrect Token Supplied Do not honour Transaction Declined Exceeds Withdrawal Limit Issuer or Switch Inoperative The card holder was not authorised. This is used in 3-D Secure Authentication.' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honor' or
        "bank_response_message" = 'PIN tries exceeded' or
        "bank_response_message" = 'No Card Record' or
        "bank_response_message" = 'Expired card' or
        "bank_response_message" = 'Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your payment has exceeded the time required to pay Merchant is not enabled for international transactions.' or
        "bank_response_message" = 'Lost Card' or
        "bank_response_message" = 'Exceeds withdrawal amount limits' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Error occured while processing your request Transaction was blocked by the Payment Server because it did not pass all risk checks.' or
        "bank_response_message" = 'Invalid Transaction' or
        "bank_response_message" = 'Lost Card or Pick-Up' or
        "bank_response_message" = 'Insufficient Funds: Your card cannot be charged due to insufficient funds. Please try another card or fund your card and try again.' or
        "bank_response_message" = 'Transaction not found' or
        "bank_response_message" = 'Restricted Merchant' or
        "bank_response_message" = 'invalid token supplied' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Wrong token or email passed' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honour' or
        "bank_response_message" = 'Suspected Fraud' or
        "bank_response_message" = 'Invalid expiry' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Function Not Permitted to Cardholder: Were sorry. We cannot charge your card due to bank restrictions. Please contact your bank or financial institution No Card Record Fee configuration not found PIN should not be more than 4 digits Failed to retrieve Card Card Issuer Unavailable Function Not Permitted to Cardholder NOAUTH_INTERNATIONAL PIN Tries Exceeded Card was not properly tokenised. Please contact support Lost Card: We are unable to verify your card or please contact your financial institution. In the meantime you can try another card Card number is invalid Exceeds Withdrawal Limi Insufficient funds Your payment has exceeded the time required to pay Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your account does not seem to have a phone number or email or hardware token provisioned. Please contact your account officer.'
        )GROUP BY time(10m)''',
        'trade_channel_system_success_alert_value',
        7,
        'trade_channel_system_alert_level_1_address',
        'trade_channel_system_alert_level_2_address',
        False,
        1
    ),

    # 通道业务交易成功数
    # 50
    (
        'Trade_Channel_Bussiness_Success_INTERSWITCH',
        '''SELECT count(distinct("id")) AS "trade_channel_bussiness_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("transaction_status" = 'SUCCESS' AND "out_channel_id" = 'INTERSWITCH' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_bussiness_success_alert_value',
        7,
        'trade_channel_bussiness_alert_level_1_address',
        'trade_channel_bussiness_alert_level_2_address',
        False,
        1
    ),

    ## 渠道 PAYSTACK

    # 通道交易数
    # 51
    (
        'Trade_Channel_PAYSTACK',
        '''SELECT count(distinct("id")) AS "channel_trade_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'PAYSTACK' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    # 通道系统交易成功数
    # 52
    (
        'Trade_Channel_System_Success_PAYSTACK',
        '''SELECT count(distinct("id")) AS "trade_channel_system_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'PAYSTACK' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} 
        AND (
        "transaction_status" = 'SUCCESS' or
        "bank_response_message" = 'Transaction not Found' or
        "bank_response_message" = 'Card Expiry is not in correct format' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Amount greater than daily transaction limit Card has expired Exceeds Withdrawal Limit Expired Card Lost Card or Pick-Up Not sufficient funds Error occurred while processing your request... Please contact merchant Insufficient Funds Token Not Generated. Customer Not Registered on Token Platform Transaction not Permitted to Cardholder Token Authorization Not Successful. Incorrect Token Supplied Do not honour Transaction Declined Exceeds Withdrawal Limit Issuer or Switch Inoperative The card holder was not authorised. This is used in 3-D Secure Authentication.' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honor' or
        "bank_response_message" = 'PIN tries exceeded' or
        "bank_response_message" = 'No Card Record' or
        "bank_response_message" = 'Expired card' or
        "bank_response_message" = 'Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your payment has exceeded the time required to pay Merchant is not enabled for international transactions.' or
        "bank_response_message" = 'Lost Card' or
        "bank_response_message" = 'Exceeds withdrawal amount limits' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Error occured while processing your request Transaction was blocked by the Payment Server because it did not pass all risk checks.' or
        "bank_response_message" = 'Invalid Transaction' or
        "bank_response_message" = 'Lost Card or Pick-Up' or
        "bank_response_message" = 'Insufficient Funds: Your card cannot be charged due to insufficient funds. Please try another card or fund your card and try again.' or
        "bank_response_message" = 'Transaction not found' or
        "bank_response_message" = 'Restricted Merchant' or
        "bank_response_message" = 'invalid token supplied' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Wrong token or email passed' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honour' or
        "bank_response_message" = 'Suspected Fraud' or
        "bank_response_message" = 'Invalid expiry' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Function Not Permitted to Cardholder: Were sorry. We cannot charge your card due to bank restrictions. Please contact your bank or financial institution No Card Record Fee configuration not found PIN should not be more than 4 digits Failed to retrieve Card Card Issuer Unavailable Function Not Permitted to Cardholder NOAUTH_INTERNATIONAL PIN Tries Exceeded Card was not properly tokenised. Please contact support Lost Card: We are unable to verify your card or please contact your financial institution. In the meantime you can try another card Card number is invalid Exceeds Withdrawal Limi Insufficient funds Your payment has exceeded the time required to pay Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your account does not seem to have a phone number or email or hardware token provisioned. Please contact your account officer.'
        )

        GROUP BY time(10m)''',
        'trade_channel_system_success_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    # 通道业务交易成功数
    # 53
    (
        'Trade_Channel_Bussiness_Success_PAYSTACK',
        '''SELECT count(distinct("id")) AS "trade_channel_bussiness_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("transaction_status" = 'SUCCESS' AND "out_channel_id" = 'PAYSTACK' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_bussiness_success_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    ## 渠道 FLUTTERWAVE

    # 通道交易数
    # 54
    (
        'Trade_Channel_FLUTTERWAVE',
        '''SELECT count(distinct("id")) AS "channel_trade_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'FLUTTERWAVE' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    # 通道系统交易成功数
    # 55
    (
        'Trade_Channel_System_Success_FLUTTERWAVE',
        '''SELECT count(distinct("id")) AS "trade_channel_system_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("out_channel_id" = 'FLUTTERWAVE' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} 
        AND (
        "transaction_status" = 'SUCCESS' or
        "bank_response_message" = 'Transaction not Found' or
        "bank_response_message" = 'Card Expiry is not in correct format' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Amount greater than daily transaction limit Card has expired Exceeds Withdrawal Limit Expired Card Lost Card or Pick-Up Not sufficient funds Error occurred while processing your request... Please contact merchant Insufficient Funds Token Not Generated. Customer Not Registered on Token Platform Transaction not Permitted to Cardholder Token Authorization Not Successful. Incorrect Token Supplied Do not honour Transaction Declined Exceeds Withdrawal Limit Issuer or Switch Inoperative The card holder was not authorised. This is used in 3-D Secure Authentication.' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honor' or
        "bank_response_message" = 'PIN tries exceeded' or
        "bank_response_message" = 'No Card Record' or
        "bank_response_message" = 'Expired card' or
        "bank_response_message" = 'Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your payment has exceeded the time required to pay Merchant is not enabled for international transactions.' or
        "bank_response_message" = 'Lost Card' or
        "bank_response_message" = 'Exceeds withdrawal amount limits' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Error occured while processing your request Transaction was blocked by the Payment Server because it did not pass all risk checks.' or
        "bank_response_message" = 'Invalid Transaction' or
        "bank_response_message" = 'Lost Card or Pick-Up' or
        "bank_response_message" = 'Insufficient Funds: Your card cannot be charged due to insufficient funds. Please try another card or fund your card and try again.' or
        "bank_response_message" = 'Transaction not found' or
        "bank_response_message" = 'Restricted Merchant' or
        "bank_response_message" = 'invalid token supplied' or
        "bank_response_message" = 'Insufficient Funds' or
        "bank_response_message" = 'Wrong token or email passed' or
        "bank_response_message" = 'Incorrect PIN' or
        "bank_response_message" = 'Do Not Honour' or
        "bank_response_message" = 'Suspected Fraud' or
        "bank_response_message" = 'Invalid expiry' or
        "bank_response_message" = 'Kindly enter the OTP sent to' or
        "bank_response_message" = 'Function Not Permitted to Cardholder: Were sorry. We cannot charge your card due to bank restrictions. Please contact your bank or financial institution No Card Record Fee configuration not found PIN should not be more than 4 digits Failed to retrieve Card Card Issuer Unavailable Function Not Permitted to Cardholder NOAUTH_INTERNATIONAL PIN Tries Exceeded Card was not properly tokenised. Please contact support Lost Card: We are unable to verify your card or please contact your financial institution. In the meantime you can try another card Card number is invalid Exceeds Withdrawal Limi Insufficient funds Your payment has exceeded the time required to pay Token Authentication Failed. Incorrect Token Supplied.' or
        "bank_response_message" = 'Your account does not seem to have a phone number or email or hardware token provisioned. Please contact your account officer.'
        )
        GROUP BY time(10m)''',
        'trade_channel_system_success_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    # 通道业务交易成功数
    # 56
    (
        'Trade_Channel_Bussiness_Success_FLUTTERWAVE',
        '''SELECT count(distinct("id")) AS "trade_channel_bussiness_success_cnt" FROM "OPAY_CHANNEL_TRANSACTION_EVENT" WHERE ("transaction_status" = 'SUCCESS' AND "out_channel_id" = 'FLUTTERWAVE' AND "pay_channel" = 'QUICKPAYMENT') AND time > {time} GROUP BY time(10m)''',
        'trade_channel_bussiness_success_alert_value',
        7,
        'trade_channel_alert_level_1_address',
        'trade_channel_alert_level_2_address',
        False,
        1
    ),

    ####### 用户相关指标 独立配置

    ## 注册用户
    # 57
    (
        'User_New_Register_User',
        '''
        SELECT count(distinct("id")) AS "new_user_cnt" FROM "OPAY_REGISTER_USER_EVENT" WHERE time > {time} GROUP BY time(10m)
        ''',
        'user_register_alert_value',
        7,
        'user_register_alert_level_1_address',
        'user_register_alert_level_2_address',
        False,
        1
    ),

    ## 登陆活跃用户数
    # 58
    (
        'User_Active_User',
        '''
        SELECT count(distinct("UID")) AS "active_user_cnt" FROM "OPAY_ACTIVE_USER_EVENT" WHERE time > {time} GROUP BY time(10m)
        ''',
        'user_active_alert_value',
        7,
        'user_active_alert_level_1_address',
        'user_active_alert_level_2_address',
        False,
        1
    ),

    ## 绑卡用户数
    # 59
    (
        'User_Bind_Card_User',
        '''
        SELECT count(distinct("user_id")) AS "bind_card_user_cnt" FROM "OPAY_PAYMENT_INSTRUMENT_EVENT" WHERE time > {time} GROUP BY time(10m)
        ''',
        'user_bind_card_alert_value',
        7,
        'user_bind_card_alert_level_1_address',
        'user_bind_card_alert_level_2_address',
        False,
        3
    ),

    ####### 短信 指标

    ## chuanglan

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


for [metrics_name, influx_db_query_sql, alert_value_name, compare_day, alert_1_level_name, alert_2_level_name,
     is_close_alert, mode] in metrcis_list:
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

