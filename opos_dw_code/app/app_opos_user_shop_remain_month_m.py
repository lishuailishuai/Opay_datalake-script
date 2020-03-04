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
    'start_date': datetime(2019, 10, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_user_shop_remain_month_m',
                  schedule_interval="00 02 1 * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_active_user_month_di_task = OssSensor(
    task_id='dwd_active_user_month_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_active_user_month_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_user_shop_remain_month_m"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_opos_user_shop_remain_month_m_sql_task(ds):
    HQL = '''


--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--00.求星期排序
with
month_order as (
  select
  amonth,bmonth,rn,max(rn) over (partition by amonth) as max
  from
  (
    select
    a.amonth,b.bmonth,row_number() over (order by b.bmonth asc) as rn
    from
      (SELECT substr(dt,0,7) as amonth FROM public_dw_dim.dim_date where dt='{pt}' group by substr(dt,0,7)) as a
    left join
      (SELECT substr(dt,0,7) as bmonth FROM public_dw_dim.dim_date where dt>='{before_320_day}' and dt<='{pt}' group by substr(dt,0,7)) as b
    on 1=1
  ) as m
),

--02.将星期与地市做全关联,求出所有星期和地市
date_city as (
  select
  a.amonth as create_month
  ,a.bmonth as remain_month
  ,(a.max - a.rn) as month_interval
  ,b.city_id
  from
    month_order as a
  left join
    (select id as city_id from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as b
  on 1=1
),

--03.新用户的留存数量
new_user_remain_cnt as (
  select
  create_month
  ,remain_month
  ,city_id
  ,count(1) as new_user_remain_cnt
  from
    (
    select 
    b.create_month as create_month
    ,a.create_month as remain_month
    ,a.city_id
    ,a.sender_id as ids
    from
    (select * from opos_dw.dwd_active_user_month_di where country_code='nal' and dt>='{before_320_day}' and dt<='{pt}' and first_order='1') as a
    inner join
    (select create_month,sender_id from opos_dw.dwd_active_user_month_di where country_code='nal' and dt='{pt}' group by create_month,sender_id) as b
    on
    a.sender_id=b.sender_id
    ) as a
  group by
  create_month
  ,remain_month
  ,city_id
),

--04.用户留存
user_remain_cnt as (
  select
  create_month
  ,remain_month
  ,city_id
  ,count(1) as user_remain_cnt
  from
    (
    select 
    b.create_month as create_month
    ,a.create_month as remain_month
    ,a.city_id
    ,a.sender_id as ids
    from
    (select * from opos_dw.dwd_active_user_month_di where country_code='nal' and dt>='{before_320_day}' and dt<='{pt}') as a
    inner join
    (select create_month,sender_id from opos_dw.dwd_active_user_month_di where country_code='nal' and dt='{pt}' group by create_month,sender_id) as b
    on
    a.sender_id=b.sender_id
    ) as a
  group by
  create_month
  ,remain_month
  ,city_id
),

--05.新创建商户留存
new_shop_remain_cnt as (
  select
  create_month
  ,remain_month
  ,city_id
  ,count(1) as new_shop_remain_cnt
  from
    (
    select 
    b.create_month as create_month
    ,a.create_month as remain_month
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select * from opos_dw.dwd_active_shop_month_di where country_code='nal' and dt>='{before_320_day}' and dt<='{pt}' and first_order='1') as a
    inner join
    (select create_month,receipt_id from opos_dw.dwd_active_shop_month_di where country_code='nal' and dt='{pt}' group by create_month,receipt_id) as b
    on
    a.receipt_id=b.receipt_id
    ) as a
  group by
  create_month
  ,remain_month
  ,city_id
),

--06.所有商户留存
shop_remain_cnt as (
  select
  create_month
  ,remain_month
  ,city_id
  ,count(1) as shop_remain_cnt
  from
    (
    select 
    b.create_month as create_month
    ,a.create_month as remain_month
    ,a.city_id
    ,a.receipt_id as ids
    from
    (select * from opos_dw.dwd_active_shop_month_di where country_code='nal' and dt>='{before_320_day}' and dt<='{pt}') as a
    inner join
    (select create_month,receipt_id from opos_dw.dwd_active_shop_month_di where country_code='nal' and dt='{pt}' group by create_month,receipt_id) as b
    on
    a.receipt_id=b.receipt_id
    ) as a
  group by
  create_month
  ,remain_month
  ,city_id
)

--07.最终将结果合并后插入到结果表中
insert overwrite table opos_dw.app_opos_user_shop_remain_month_m partition(country_code,dt)
select
0 as id

,v1.create_month

,v1.month_interval

,v1.remain_month

,v1.city_id
,nvl(v2.name,'-') as city_name
,nvl(v2.country,'-') as country

,nvl(v3.new_user_remain_cnt,0) as new_user_remain_cnt
,nvl(v4.user_remain_cnt,0) as user_remain_cnt
,nvl(v5.new_shop_remain_cnt,0) as new_shop_remain_cnt
,nvl(v6.shop_remain_cnt,0) as shop_remain_cnt

,'nal' as country_code
,'{pt}' as dt
from
  date_city as v1
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as v2
on v1.city_id=v2.id
left join
  new_user_remain_cnt as v3
on v1.create_month=v3.create_month
  and v1.remain_month=v3.remain_month
  and v1.city_id=v3.city_id
left join
  user_remain_cnt as v4
on v1.create_month=v4.create_month
  and v1.remain_month=v4.remain_month
  and v1.city_id=v4.city_id
left join
  new_shop_remain_cnt as v5
on v1.create_month=v5.create_month
  and v1.remain_month=v5.remain_month
  and v1.city_id=v5.city_id
left join
  shop_remain_cnt as v6
on v1.create_month=v6.create_month
  and v1.remain_month=v6.remain_month
  and v1.city_id=v6.city_id;




'''.format(
        pt=ds,
        before_320_day=airflow.macros.ds_add(ds, -320),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opos_user_shop_remain_month_m_sql_task(ds)

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


app_opos_user_shop_remain_month_m_task = PythonOperator(
    task_id='app_opos_user_shop_remain_month_m_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_active_user_month_di_task >> app_opos_user_shop_remain_month_m_task

# 查看任务命令
# airflow list_tasks app_opos_user_shop_remain_month_m -sd /root/feng.yuan/app_opos_user_shop_remain_month_m.py
# 测试任务命令
# airflow test app_opos_user_shop_remain_month_m app_opos_user_shop_remain_month_m_task 2019-11-28 -sd /root/feng.yuan/app_opos_user_shop_remain_month_m.py


