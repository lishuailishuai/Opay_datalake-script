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

dag = airflow.DAG('app_user_shop_remain_week_w',
                  schedule_interval="00 04 * * 1",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_active_user_week_di_task = OssSensor(
    task_id='dwd_active_user_week_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_active_user_week_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_user_shop_remain_week_w"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "6000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_user_shop_remain_week_w_sql_task(ds):
    HQL = '''


--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;


--01.新用户留存
with
new_user_remain_cnt as (
  select
  create_year_week
  ,remain_year_week
  ,city_id
  ,count(1) as new_user_remain_cnt
  from
    (
    select 
    b.combine_year_week as create_year_week
    ,a.combine_year_week as remain_year_week
    ,a.city_id
    ,a.sender_id as ids
    from
    (select * from opos_dw.dwd_active_user_week_di where country_code='nal' and dt>='{before_75_day}' and dt<='{after_6_day}') as a
    inner join
    (select * from opos_dw.dwd_active_user_week_di where country_code='nal' and dt='{after_6_day}' and first_order='1') as b
    on
    a.city_id=b.city_id
    and a.sender_id=b.sender_id
    ) as a
  group by
  create_year_week
  ,remain_year_week
  ,city_id
),

--02.用户留存
user_remain_cnt as (
  select
  create_year_week
  ,remain_year_week
  ,city_id
  ,count(1) as user_remain_cnt
  from
    (
    select 
    b.combine_year_week as create_year_week
    ,a.combine_year_week as remain_year_week
    ,a.city_id
    ,a.sender_id as ids
    from
    (select * from opos_dw.dwd_active_user_week_di where country_code='nal' and dt>='{before_75_day}' and dt<='{after_6_day}') as a
    inner join
    (select * from opos_dw.dwd_active_user_week_di where country_code='nal' and dt='{after_6_day}') as b
    on
    a.city_id=b.city_id
    and a.sender_id=b.sender_id
    ) as a
  group by
  create_year_week
  ,remain_year_week
  ,city_id
),

--03.新创建商户留存
new_shop_remain_cnt as (
  select
  create_year_week
  ,remain_year_week
  ,city_id
  ,count(1) as new_shop_remain_cnt
  from
    (
    select 
    b.combine_year_week as create_year_week
    ,a.combine_year_week as remain_year_week
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select * from opos_dw.dwd_active_shop_week_di where country_code='nal' and dt>='{before_75_day}' and dt<='{after_6_day}') as a
    inner join
    (select * from opos_dw.dwd_active_shop_week_di where country_code='nal' and dt='{after_6_day}' and first_order='1') as b
    on
    a.city_id=b.city_id
    and a.receipt_id=b.receipt_id
    ) as a
  group by
  create_year_week
  ,remain_year_week
  ,city_id
),

--04.所有商户留存
shop_remain_cnt as (
  select
  create_year_week
  ,remain_year_week
  ,city_id
  ,count(1) as shop_remain_cnt
  from
    (
    select 
    b.combine_year_week as create_year_week
    ,a.combine_year_week as remain_year_week
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select * from opos_dw.dwd_active_shop_week_di where country_code='nal' and dt>='{before_75_day}' and dt<='{after_6_day}') as a
    inner join
    (select * from opos_dw.dwd_active_shop_week_di where country_code='nal' and dt='{after_6_day}') as b
    on
    a.city_id=b.city_id
    and a.receipt_id=b.receipt_id
    ) as a
  group by
  create_year_week
  ,remain_year_week
  ,city_id
)

--05.最终将结果合并后插入到结果表中
insert overwrite table opos_dw.app_user_shop_remain_week_w partition(country_code,dt)
select
0 as id
,substr(v1.create_year_week,0,4) as create_year
,cast(substr(v1.create_year_week,5,2) as int) as create_week
,v1.create_year_week

,substr(v1.remain_year_week,0,4) as remain_year
,cast(substr(v1.remain_year_week,5,2) as int) as remain_week
,v1.remain_year_week

,v1.city_id
,nvl(v2.name,'-') as city_name
,nvl(v2.country,'-') as country

,v1.new_user_remain_cnt
,v1.user_remain_cnt
,v1.new_shop_remain_cnt
,v1.shop_remain_cnt

,'nal' as country_code
,'{after_6_day}' as dt
from
  (
  select
  nvl(m.create_year_week,n.create_year_week) as create_year_week
  ,nvl(m.remain_year_week,n.remain_year_week) as remain_year_week
  ,nvl(m.city_id,n.city_id) as city_id
  ,nvl(m.new_user_remain_cnt,0) as new_user_remain_cnt
  ,nvl(m.user_remain_cnt,0) as user_remain_cnt
  ,nvl(n.new_shop_remain_cnt,0) as new_shop_remain_cnt
  ,nvl(n.shop_remain_cnt,0) as shop_remain_cnt
  from
    (
    select
    a.create_year_week
    ,a.remain_year_week
    ,a.city_id
    ,a.user_remain_cnt
    ,nvl(b.new_user_remain_cnt,0) as new_user_remain_cnt
    from
    (select * from user_remain_cnt) as a
    left join
    (select * from new_user_remain_cnt) as b
    ) as m
  full join
    (
    select
    c.create_year_week
    ,c.remain_year_week
    ,c.city_id
    ,c.shop_remain_cnt
    ,nvl(d.new_shop_remain_cnt,0) as new_shop_remain_cnt
    from
    (select * from shop_remain_cnt) as c
    left join
    (select * from new_shop_remain_cnt) as d
    ) as n
  on
    m.create_year_week=n.create_year_week
    and m.remain_year_week=n.remain_year_week
    and m.city_id=n.city_id
  ) as v1
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{after_6_day}') as v2
on
v1.city_id=v2.id;



'''.format(
        pt=ds,
        after_6_day=airflow.macros.ds_add(ds, +6),
        before_75_day=airflow.macros.ds_add(ds, -75),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_user_shop_remain_week_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    after_6_day = airflow.macros.ds_add(ds, +6)
    TaskTouchzSuccess().countries_touchz_success(after_6_day, db_name, table_name, hdfs_path, "true", "true")


app_user_shop_remain_week_w_task = PythonOperator(
    task_id='app_user_shop_remain_week_w_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_active_user_week_di_task >> app_user_shop_remain_week_w_task



