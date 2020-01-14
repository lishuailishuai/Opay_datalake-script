# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta, time
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_shop_target_week_w',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

app_opos_shop_target_week_w_task = OssSensor(
    task_id='app_opos_shop_target_week_w_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/app_opos_shop_target_week_w",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 删除mysql昨日数据,当当日数据是周一时,不删除 ---------------------------------------##

week = time.strptime('{{ds}}',"%Y-%m-%d")[6]

#判断是否是周一并生成对应sql
if week == 0 :
    delete_sql = """
DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{ds}';
--DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{before_1_day}';
            """.format(
        ds='{{ds}}',
        before_1_day='{{ macros.ds_add(ds, -1) }}'
    )
else :
    delete_sql = """
DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{ds}';
DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{before_1_day}';
            """.format(
        ds='{{ds}}',
        before_1_day='{{ macros.ds_add(ds, -1) }}'
    )

#执行删除操作
drop_mysql_yesterday_data = MySqlOperator(
    task_id='drop_mysql_yesterday_data',
    sql=delete_sql,
    mysql_conn_id='mysql_dw',
    dag=dag)

##----------------------------------------- 将最新数据插入到mysql ---------------------------------------##

insert_mysql_today_data = HiveToMySqlTransfer(
    task_id='insert_mysql_today_data',
    sql="""
select
id
,shop_id
,opay_id
,shop_name
,opay_account
,create_week
,city_code
,city_name
,country
,hcm_id
,hcm_name
,cm_id
,cm_name
,rm_id
,rm_name
,bdm_id
,bdm_name
,bd_id
,bd_name
,order_cnt
,cashback_order_cnt
,cashback_fail_order_cnt
,cashback_order_gmv
,cashback_per_order_amt
,cashback_per_people_amt
,cashback_people_cnt
,cashback_first_people_cnt
,cashback_zero_order_cnt
,cashback_amt
,reduce_order_cnt
,reduce_zero_order_cnt
,reduce_amt
,reduce_order_gmv
,reduce_people_cnt
,reduce_first_people_cnt
,bonus_order_cnt
,order_people
,not_first_order_people
,first_order_people
,first_bonus_order_people
,order_gmv
,bonus_order_gmv
,bonus_order_amt
,sweep_amt
,bonus_order_people
,bonus_order_times
,order_create_cnt
,order_pay_cnt
,order_fail_cnt
,order_pending_cnt
,coupon_order_cnt
,coupon_order_people
,coupon_first_order_people
,coupon_pay_amount
,coupon_order_gmv
,coupon_discount_amount
,coupon_useless_order_cnt
,coupon_useless_order_people
,coupon_useless_pay_amount
,coupon_useless_order_gmv
,bak1
,bak2
,bak3
,bak4
,bak5
,bak6
,bak7
,bak8
,bak9
,bak10
,bak11
,bak12
,bak13
,bak14
,bak15
,bak16
,bak17
,bak18
,bak19
,bak20
,country_code
,dt
from
opos_dw.app_opos_shop_target_week_w
where
country_code = 'nal'
and dt='{ds}'

    """.format(
        ds='{{ds}}',
        before_1_day ='{{ macros.ds_add(ds, -1) }}'
    ),
    mysql_conn_id='mysql_dw',
    mysql_table='app_opos_shop_target_week_w',
    dag=dag)

##----------------------------------------- 最后执行流程 ---------------------------------------##

app_opos_shop_target_week_w_task >> drop_mysql_yesterday_data >> insert_mysql_today_data









