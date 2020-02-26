# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2020, 2, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_cico_sum_ng_h',
                  schedule_interval="35 * * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_cico_sum_ng_h"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查当前小时的依赖
dwd_opay_cico_record_hi_check_task = OssSensor(
    task_id='dwd_opay_cico_record_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cico_record_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查上一个小时的依赖
dwd_opay_cico_record_hi_pre_check_task = OssSensor(
    task_id='dwd_opay_cico_record_hi_pre_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cico_record_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=-1),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=-1)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##


def app_opay_cico_sum_ng_h_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

insert overwrite table opay_dw.app_opay_cico_sum_ng_h partition(country_code, dt, hour)
select
  create_date_hour
  ,sub_service_type
  ,state
  ,region
  ,order_status

  ,count(1) as order_cnt
  ,sum(amount) as order_amt

  ,'NG' as country_code
  ,date_format(create_date_hour, 'yyyy-MM-dd') as dt
  ,substring(create_date_hour, 12, 2) as hour
from
  (
  select
    a.create_date_hour
    ,a.sub_service_type
    ,a.state
    ,b.region
    ,a.order_status
    ,a.amount
  from
    (
    select 
      date_format(create_time, 'yyyy-MM-dd HH') as create_date_hour
      ,sub_service_type
      ,state
      ,order_status
      ,amount
      ,row_number() over(partition by order_no order by update_time desc) rn
    from
      opay_dw.dwd_opay_cico_record_hi
    where
      country_code = 'NG'
      and concat(dt,' ',hour) >= date_format(default.localTime("{config}", 'NG', '{v_date}', -1), 'yyyy-MM-dd HH')
      and concat(dt,' ',hour) <= date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH')
      and create_time >= date_format(default.localTime("{config}", 'NG', '{v_date}', -1), 'yyyy-MM-dd HH') 
      and create_time <= date_format(default.localTime("{config}", 'NG', '{v_date}', 1), 'yyyy-MM-dd HH') 
    ) as a
  left join
    (
    select
      state
      , region
    from 
      opay_dw.dim_opay_region_state_mapping_df
    where 
      dt = if('{pt}' <= '2020-02-10', '2020-02-10', '{pt}')
    ) as b
  on
    a.state = b.state
  where
    a.rn = 1
  ) as c
group by
  create_date_hour
  ,sub_service_type
  ,state
  ,region
  ,order_status

;



    '''.format(
        pt=ds,
        v_date=v_date,
        table=table_name,
        db=db_name,
        config=config

    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
            alter语句: alter_partition()
            删除分区: delete_partition()
            生产success: touchz_success()

        #参数
            is_countries_online --是否开通多国家业务 默认(true 开通)
            db_name --hive 数据库的名称
            table_name --hive 表的名称
            data_oss_path --oss 数据目录的地址
            is_country_partition --是否有国家码分区,[默认(true 有country_code分区)]
            is_result_force_exist --数据是否强行产出,[默认(true 必须有数据才生成_SUCCESS)] false 数据没有也生成_SUCCESS 
            execute_time --当前脚本执行时间(%Y-%m-%d %H:%M:%S)
            is_hour_task --是否开通小时级任务,[默认(false)]
            frame_type --模板类型(只有 is_hour_task:'true' 时生效): utc 产出分区为utc时间，local 产出分区为本地时间,[默认(utc)]。

        #读取sql
            %_sql(ds,v_hour)

    """

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
            "is_hour_task": "true",
            "frame_type": "local"
        }
    ]

    cf = CountriesPublicFrame_dev(args)

    # 删除分区
    # cf.delete_partition()

    # print(dwd_opay_life_payment_record_hi_sql_task(ds, v_date))

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_cico_sum_ng_h_sql_task(ds, v_date)

    # _sql = "\n" + dwd_opay_life_payment_record_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


app_opay_cico_sum_ng_h_task = PythonOperator(
    task_id='app_opay_cico_sum_ng_h_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwd_opay_cico_record_hi_check_task >> app_opay_cico_sum_ng_h_task
dwd_opay_cico_record_hi_pre_check_task >> app_opay_cico_sum_ng_h_task


