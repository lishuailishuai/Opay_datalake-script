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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'lili.chen',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dim_oride_driver_base', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
ods_sqoop_base_data_driver_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_driver_df_prev_day_tesk",
      table="ods_sqoop_base_data_driver_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )


#依赖前一天分区
ods_sqoop_base_data_driver_extend_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_driver_extend_df_prev_day_tesk",
      table="ods_sqoop_base_data_driver_extend_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态 
      dag=dag
    )

dim_oride_city_prev_day_tesk = UFileSensor(
    task_id='dim_oride_city_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
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
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------## 

db_name = "oride_dw"
table_name="dim_oride_driver_base"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 
def dim_oride_driver_base_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    SELECT dri.driver_id,
           ext.city_id,
           --所属城市ID
    
           phone_number,
           --手机号
    
           password,
           --密码
    
           opay_account,
           --opay 账号
    
           plate_number,
           --车牌号
    
           driver_name,
           --真实姓名
    
           birthday,
           --生日
    
           gender,
           --性别
    
           government,
           --Local Government
    
           country,
           --国家
    
           cit.city_name,
           --城市名称
    
           black,
           --黑名单0正常1删除
    
           group_id,
           --所属组id
    
           serv_mode,
           --服务模式 (0: no service, 1: in service)
    
           serv_status,
           --服务状态 (0: wait assign, 1: pick up, 2: send)
    
           from_unixtime(register_time,'yyyy-MM-dd HH:mm:ss') as register_time,
           --注册时间
    
           from_unixtime(login_time,'yyyy-MM-dd HH:mm:ss') as login_time,
           --最后登陆时间
    
           is_bind,
           --状态 0 未绑定 1 已绑定
    
           from_unixtime(first_bind_time,'yyyy-MM-dd HH:mm:ss') as first_bind_time,
           --初次绑定时间
    
           block,
           --后台管理司机接单状态(0: 允许 1:不允许)
    
           ext.product_id,
           --1 专车 2 快车 3 Otrike
    
           local_gov_ids,
           --行会ID,json
    
           dri.updated_at,
           --最后更新时间
    
           fault,
           --正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
    
           LANGUAGE, --客户端语言
    
           'nal' AS country_code,
           --国家码字段
    
            '{pt}' as dt
    FROM
      (SELECT id AS driver_id,
              --司机 ID
    
              phone_number,
              --手机号
    
              password,
              --密码
    
              opay_account,
              --opay 账号
    
              plate_number,
              --车牌号
    
              real_name as driver_name,
              --真实姓名
    
              birthday,
              --生日
    
              gender,
              --性别
    
              government,
              --Local Government
    
              country,
              --国家
    
              city as city_name,
              --城市名称
    
              black,
              --黑名单0正常1删除
    
              group_id,
              --所属组id
    
              updated_at
       FROM oride_dw_ods.ods_sqoop_base_data_driver_df
       WHERE  dt = '{pt}') dri
    LEFT OUTER JOIN
      (SELECT id AS driver_id,
              --司机 ID
    
              serv_mode,
              --服务模式 (0: no service, 1: in service)
    
              serv_status,
              --服务状态 (0: wait assign, 1: pick up, 2: send)
    
              register_time,
              --注册时间
    
              login_time,
              --最后登陆时间
    
              is_bind,
              --状态 0 未绑定 1 已绑定
    
              first_bind_time,
              --初次绑定时间
    
              block,
              --后台管理司机接单状态(0: 允许 1:不允许)
    
              serv_type AS product_id,
              --1 专车 2 快车
    
              local_gov_ids,
              --行会ID,json
    
              fault,
              --正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
    
              city_id,
              --所属城市ID
    
              LANGUAGE --客户端语言
    FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
       WHERE dt = '{pt}') ext ON dri.driver_id=ext.driver_id
    LEFT OUTER JOIN
    (select * from oride_dw.dim_oride_city where dt = '{pt}' and country_code='NG') cit
    ON cit.city_id=ext.city_id
    -- where ext.city_id<>'999001' --去除测试数据
    -- and dri.driver_id not in(3835,
    -- 3963,
    -- 3970,
    -- 4702,
    -- 5559,
    -- 5902,
    -- 7669,
    -- 29105, --以上都是录错城市的司机
    -- 10722, --测试数据
    -- 1)    --北京城市测试数据
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('nal')
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


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_oride_driver_base_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_oride_driver_base_task = PythonOperator(
    task_id='dim_oride_driver_base_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_driver_df_prev_day_tesk>>ods_sqoop_base_data_driver_extend_df_prev_day_tesk>>dim_oride_city_prev_day_tesk>>dim_oride_driver_base_task


