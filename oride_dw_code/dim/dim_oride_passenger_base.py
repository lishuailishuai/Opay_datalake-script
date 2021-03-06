# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesAppFrame import CountriesAppFrame

import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'lishuai',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dim_oride_passenger_base', 
    schedule_interval="25 00 * * *",
    default_args=args,
    )
##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dim_oride_passenger_base"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 依赖 ---------------------------------------## 

dwd_oride_passenger_extend_df_prev_day_task = OssSensor(
        task_id='dwd_oride_passenger_extend_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_passenger_extend_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dwd_oride_passenger_df_prev_day_task = OssSensor(
        task_id='dwd_oride_passenger_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_passenger_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 


def dim_oride_passenger_base_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE {db}.{table} partition(country_code,dt)
SELECT t1.passenger_id,
       --乘客ID

       t1.phone_number,
       --手机号

       t1.first_name,
       --名

       t1.last_name,
       --姓

       t1.promoter_code,
       --推广员代码

       t1.updated_at,
       --最后更新时间

       t2.avg_score,
       -- 平均评分

       t2.total_score,
       -- 总评分

       t2.score_times,
       -- 评分次数

       t2.bonus,
       -- 奖励金

       t2.balance,
       -- 余额

       from_unixtime(t2.register_time,'yyyy-MM-dd HH:mm:ss') AS register_time,
       -- 注册时间

       from_unixtime(t2.login_time,'yyyy-MM-dd HH:mm:ss') AS login_time,
       -- 最后登陆时间

       t2.inviter_role,
       --

       t2.inviter_id,
       --

       t2.invite_num,
       --

       t2.invite_complete_num,
       --

       t2.invite_award,
       --
--t2.updated_at, -- 最后更新时间,可以精确到时间

       t2.pay_type,
       -- user auto pay settings(-1: not set 0: manual payment 1: auto payment)

       t2.city_id,
       -- 注册城市

       t2.language,
       -- 客户端语言

       null as device_id, --设备ID
       from_unixtime(t2.protocol_time,'yyyy-MM-dd HH:mm:ss') as protocol_time, --签约/解约时间

       t1.country_code,
       '{pt}' AS dt
FROM
  (SELECT passenger_id,
          --'乘客ID'

          phone_number,
          -- '手机号'

          first_name,
          -- '名'

          last_name,
          -- '姓'

          promoter_code,
          --'推广员代码'

          updated_at,
          --'最后更新时间'

          'nal' AS country_code --国家码字段

   FROM oride_dw.dwd_oride_passenger_df
   WHERE dt= '{pt}') t1
LEFT OUTER JOIN
  (SELECT passenger_id,
          -- 用户 ID

          avg_score,
          -- 平均评分

          total_score,
          -- 总评分

          score_times,
          -- 评分次数

          bonus,
          -- 奖励金

          balance,
          -- 余额

          register_time,
          -- 注册时间

          login_time,
          -- 最后登陆时间

          inviter_role,
          --

          inviter_id,
          --

          invite_num,
          --

          invite_complete_num,
          --

          invite_award,
          --

          updated_at,
          -- 最后更新时间

          pay_type,
          -- user auto pay settings(-1: not set 0: manual payment 1: auto payment)

          city_id,
          -- 注册城市

          LANGUAGE, -- 客户端语言
          protocol_time --签约/解约时间
FROM oride_dw.dwd_oride_passenger_extend_df
   WHERE dt= '{pt}') t2 ON t1.passenger_id=t2.passenger_id;
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL


#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    SELECT count(1)-count(distinct passenger_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)
    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dim_oride_passenger_base_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

dim_oride_passenger_base_task = PythonOperator(
    task_id='dim_oride_passenger_base_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}',
        'owner':'{{owner}}'
    },
    dag=dag
)

dwd_oride_passenger_df_prev_day_task>>dim_oride_passenger_base_task
dwd_oride_passenger_extend_df_prev_day_task>>dim_oride_passenger_base_task

