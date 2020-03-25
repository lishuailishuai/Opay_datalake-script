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
from utils.get_local_time import GetLocalTime


args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2020, 3, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_otrade_bd_info_hf',
                  schedule_interval="25 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dim_otrade_bd_info_hf"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查最新的商户表的依赖
dwd_otrade_bd_admin_users_hf_locale_task = OssSensor(
    task_id='dwd_otrade_bd_admin_users_hf_locale_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_bd_admin_users_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(time_zone=time_zone,gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(time_zone=time_zone,gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,execution_date,**op_kwargs):

    dag_ids=dag.dag_id

    #监控国家
    v_country_code='NG'

    #时间偏移量
    v_gap_hour=0

    v_date=GetLocalTime("otrade",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['date']
    v_hour=GetLocalTime("otrade",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['hour']

    #小时级监控
    tb_hour_task = [
        {"dag":dag,"db": "otrade_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,pt=v_date,now_hour=v_hour), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

def dim_otrade_bd_info_hf_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

--1.从ods中取出最近一个分区的全量数据,bd数据是每小时全量
with
all_bd_info as (
  select
    id
    ,username
    ,mobile
    ,job_id
    ,case
      when job_id=1 then 'HCM'
      when job_id=2 then 'CM'
      when job_id=3 then 'BDM'
      when job_id=4 then 'BD'
      else '-'
    end as job_name
    ,leader_id
    ,staff_id
    ,name
    ,password
    ,department_id
    ,email
    ,status
    ,remember_token
    ,avatar
    ,fcm_token
    ,city_id
    ,created_at
    ,updated_at
    ,row_number() over(partition by id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
  from
    otrade_dw.dwd_otrade_bd_admin_users_hf
  where 
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.分别取出每个层级的全量数据
hcm_info as (
  select
    id
    ,name
    ,job_id
    ,job_name
    ,mobile
    ,username
    ,leader_id
    ,department_id
    ,email
    ,status
    ,city_id
    ,created_at
    ,updated_at

    ,id as hcm_id
    ,name as hcm_name
    ,0 as cm_id
    ,'-' as cm_name
    ,0 as bdm_id
    ,'-' as bdm_name
    ,0 as bd_id
    ,'-' as bd_name
  from
    all_bd_info
  where
    job_id = 1
),

cm_info as (
  select
    v1.id
    ,v1.name
    ,v1.job_id
    ,v1.job_name
    ,v1.mobile
    ,v1.username
    ,v1.leader_id
    ,v1.department_id
    ,v1.email
    ,v1.status
    ,v1.city_id
    ,v1.created_at
    ,v1.updated_at

    ,nvl(v2.hcm_id,0) as hcm_id
    ,nvl(v2.hcm_name,'-') as hcm_name
    ,v1.id as cm_id
    ,v1.name as cm_name
    ,0 as bdm_id
    ,'-' as bdm_name
    ,0 as bd_id
    ,'-' as bd_name
  from
    (select * from all_bd_info where job_id = 2) as v1
  left join
    hcm_info as v2
  on
    v1.leader_id = v2.id
),

bdm_info as (
  select
    id
    ,name
    ,job_id
    ,job_name
    ,mobile
    ,username
    ,leader_id
    ,department_id
    ,email
    ,status
    ,city_id
    ,created_at
    ,updated_at

    ,nvl(v2.hcm_id,0) as hcm_id
    ,nvl(v2.hcm_name,'-') as hcm_name
    ,nvl(v2.cm_id,0) as cm_id
    ,nvl(v2.cm_name,'-') as cm_name
    ,v1.id as bdm_id
    ,v1.name as bdm_name
    ,0 as bd_id
    ,'-' as bd_name
  from
    (select * from all_bd_info where job_id = 3) as v1
  left join
    cm_info as v2
  on
    v1.leader_id = v2.id
),

bd_info as (
  select
    id
    ,name
    ,job_id
    ,job_name
    ,mobile
    ,username
    ,leader_id
    ,department_id
    ,email
    ,status
    ,city_id
    ,created_at
    ,updated_at

    ,nvl(v2.hcm_id,0) as hcm_id
    ,nvl(v2.hcm_name,'-') as hcm_name
    ,nvl(v2.cm_id,0) as cm_id
    ,nvl(v2.cm_name,'-') as cm_name
    ,nvl(v2.bdm_id,0) as bdm_id
    ,nvl(v2.bdm_name,'-') as bdm_name
    ,v1.id as bd_id
    ,v1.name as bd_name
  from
    (select * from all_bd_info where job_id = 4) as v1
  left join
    bdm_info as v2
  on
    v1.leader_id = v2.id
),

else_info as (
  select
    id
    ,name
    ,job_id
    ,job_name
    ,mobile
    ,username
    ,leader_id
    ,department_id
    ,email
    ,status
    ,city_id
    ,created_at
    ,updated_at

    ,0 as hcm_id
    ,'-' as hcm_name
    ,0 as cm_id
    ,'-' as cm_name
    ,0 as bdm_id
    ,'-' as bdm_name
    ,0 as bd_id
    ,'-' as bd_name
  from
    all_bd_info 
  where
    job_id = 0
),

--3.将关联好的结果融合到一起
unoin_result as (
  select * from hcm_info
  union all
  select * from cm_info
  union all
  select * from bdm_info
  union all
  select * from bd_info
  union all
  select * from else_info
)

--4.最后将数据插入info表中
insert overwrite table otrade_dw.dim_otrade_bd_info_hf partition(country_code,dt,hour)
select 
  id
  ,name
  ,job_id
  ,job_name
  
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name
  
  ,mobile
  ,username
  ,leader_id
  ,department_id
  ,email
  ,status
  ,city_id
  ,created_at
  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  unoin_result;



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

   # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dim_otrade_bd_info_hf_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dim_otrade_bd_info_hf_task = PythonOperator(
    task_id='dim_otrade_bd_info_hf_task',
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

dwd_otrade_bd_admin_users_hf_locale_task >> dim_otrade_bd_info_hf_task


