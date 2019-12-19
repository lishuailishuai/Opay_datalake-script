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

dag = airflow.DAG('app_user_shop_remain_day_d',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_pre_opos_payment_order_di_task = OssSensor(
    task_id='dwd_pre_opos_payment_order_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_pre_opos_payment_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_user_shop_remain_day_d"
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

def app_user_shop_remain_day_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;


--01.新用户留存
with
new_user_remain_cnt as (
  select
  create_date
  ,remain_date
  ,city_id
  ,count(1) as new_user_remain_cnt
  from
    (
    select 
    b.dt as create_date
    ,a.dt as remain_date
    ,a.city_id
    ,a.sender_id as ids
    from
    (select dt,city_id,sender_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt>='{before_29_day}' and dt<='{pt}' and first_order='1' group by dt,city_id,sender_id) as a
    inner join
    (select dt,city_id,sender_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt='{pt}' group by dt,city_id,sender_id) as b
    on
    a.city_id=b.city_id
    and a.sender_id=b.sender_id
    ) as a
  group by
  create_date
  ,remain_date
  ,city_id
),

--02.用户留存
user_remain_cnt as (
  select
  create_date
  ,remain_date
  ,city_id
  ,count(1) as user_remain_cnt
  from
    (
    select 
    b.dt as create_date
    ,a.dt as remain_date
    ,a.city_id
    ,a.sender_id as ids
    from
    (select dt,city_id,sender_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt>='{before_29_day}' and dt<='{pt}' group by dt,city_id,sender_id) as a
    inner join
    (select dt,city_id,sender_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt='{pt}' group by dt,city_id,sender_id) as b
    on
    a.city_id=b.city_id
    and a.sender_id=b.sender_id
    ) as a
  group by
  create_date
  ,remain_date
  ,city_id
),

--03.新创建商户留存
new_shop_remain_cnt as (
  select
  create_date
  ,remain_date
  ,city_id
  ,count(1) as new_shop_remain_cnt
  from
    (
    select 
    b.dt as create_date
    ,a.dt as remain_date
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select dt,city_id,receipt_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt>='{before_29_day}' and dt<='{pt}' and dt=created_at group by dt,city_id,receipt_id) as a
    inner join
    (select dt,city_id,receipt_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt='{pt}' group by dt,city_id,receipt_id) as b
    on
    a.city_id=b.city_id
    and a.receipt_id=b.receipt_id
    ) as a
  group by
  create_date
  ,remain_date
  ,city_id
),

--04.所有商户留存
shop_remain_cnt as (
  select
  create_date
  ,remain_date
  ,city_id
  ,count(1) as shop_remain_cnt
  from
    (
    select 
    b.dt as create_date
    ,a.dt as remain_date
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select dt,city_id,receipt_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt>='{before_29_day}' and dt<='{pt}' group by dt,city_id,receipt_id) as a
    inner join
    (select dt,city_id,receipt_id from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt='{pt}' group by dt,city_id,receipt_id) as b
    on
    a.city_id=b.city_id
    and a.receipt_id=b.receipt_id
    ) as a
  group by
  create_date
  ,remain_date
  ,city_id
)

--05.插入日留存数据
insert overwrite table opos_dw.app_user_shop_remain_day_d partition(country_code,dt)
select
0 as id
,v1.create_date

,v1.remain_date

,v1.city_id
,nvl(v2.name,'-') as city_name
,nvl(v2.country,'-') as country

,v1.new_user_remain_cnt
,v1.user_remain_cnt
,v1.new_shop_remain_cnt
,v1.shop_remain_cnt

,'nal' as country_code
,'{pt}' as dt
from
  (
  select
  nvl(m.create_date,n.create_date) as create_date
  ,nvl(m.remain_date,n.remain_date) as remain_date

  ,nvl(m.city_id,n.city_id) as city_id

  ,nvl(m.new_user_remain_cnt,0) as new_user_remain_cnt
  ,nvl(m.user_remain_cnt,0) as user_remain_cnt
  ,nvl(n.new_shop_remain_cnt,0) as new_shop_remain_cnt
  ,nvl(n.shop_remain_cnt,0) as shop_remain_cnt
  from
    (
    select
    a.create_date
    ,a.remain_date
    ,a.city_id
    ,a.user_remain_cnt
    ,nvl(b.new_user_remain_cnt,0) as new_user_remain_cnt
    from
    (select * from user_remain_cnt) as a
    left join
    (select * from new_user_remain_cnt) as b
    on  a.create_date=b.create_date
    and a.remain_date=b.remain_date
    and a.city_id=b.city_id
    ) as m
  full join
    (
    select
    c.create_date
    ,c.remain_date
    ,c.city_id
    ,c.shop_remain_cnt
    ,nvl(d.new_shop_remain_cnt,0) as new_shop_remain_cnt
    from
    (select * from shop_remain_cnt) as c
    left join
    (select * from new_shop_remain_cnt) as d
    on  c.create_date=d.create_date
    and c.remain_date=d.remain_date
    and c.city_id=d.city_id
    ) as n
  on
    m.create_date=n.create_date
    and m.remain_date=n.remain_date
    and m.city_id=n.city_id
  ) as v1
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as v2
on
v1.city_id=v2.id;





'''.format(
        pt=ds,
        after_6_day=airflow.macros.ds_add(ds, +6),
        before_29_day=airflow.macros.ds_add(ds, -29),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_user_shop_remain_day_d_sql_task(ds)

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
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_user_shop_remain_day_d_task = PythonOperator(
    task_id='app_user_shop_remain_day_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> app_user_shop_remain_day_d_task

# 查看任务命令
# airflow list_tasks app_user_shop_remain_day_d -sd /home/feng.yuan/app_user_shop_remain_day_d.py
# 测试任务命令
# airflow test app_user_shop_remain_day_d app_user_shop_remain_day_d_task 2019-11-24 -sd /home/feng.yuan/app_user_shop_remain_day_d.py

