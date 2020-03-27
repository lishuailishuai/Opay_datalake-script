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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lishuai',
    'start_date': datetime(2019, 10, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_extend_df',
                  schedule_interval="20 00 * * *",
                  default_args=args,
                  )



##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_driver_extend_df"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_base_data_driver_extend_hi_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_driver_extend_hi_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="oride_binlog/oride_db.oride_data.data_driver_extend",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前天分区
dwd_oride_driver_extend_df_prev_day_tesk = OssSensor(
    task_id='dwd_oride_driver_extend_df_prev_day_tesk',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_extend_df/country_code=nal",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_driver_extend_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

       insert overwrite table {db}.{table} partition(country_code,dt)

        SELECT
          
          nvl(data_driver.id,data_driver_bef.id),--'司机 ID', 
          nvl(data_driver.serv_mode,data_driver_bef.serv_mode),--服务模式 (0: no service, 1: in service, 99:招手停)', 
          nvl(data_driver.serv_status,data_driver_bef.serv_status),--'服务状态 (0: wait assign, 1: pick up, 2:wait, 3: send, 4:arrive, 5:pick order)', 
          nvl(data_driver.order_rate,data_driver_bef.order_rate),--'接单率', 
          nvl(data_driver.assign_order,data_driver_bef.assign_order),--'派单数量', 
          nvl(data_driver.take_order,data_driver_bef.take_order),--'接单数量', 
          nvl(data_driver.avg_score,data_driver_bef.avg_score),--'平均评分', 
          nvl(data_driver.total_score,data_driver_bef.total_score),--'总评分', 
          nvl(data_driver.score_times,data_driver_bef.score_times),--'评分次数', 
          nvl(data_driver.last_order_id,data_driver_bef.last_order_id),--'最近一个订单的ID', 
          case when data_driver.register_time=0 then 0
          when data_driver.register_time!=0 and data_driver.register_time is not null then data_driver.register_time+3600
          else data_driver_bef.register_time end,--'注册时间', 
          
          case when
          data_driver.login_time=0 then 0
          when data_driver.login_time!=0 and data_driver.login_time is not null then data_driver.login_time+3600
          else data_driver_bef.login_time end,--'最后登陆时间',
          
          nvl(data_driver.is_bind,data_driver_bef.is_bind),--'状态 0 未绑定 1 已绑定', 
          case when
          data_driver.first_bind_time=0 then 0
          when data_driver.first_bind_time!=0 and data_driver.first_bind_time is not null then data_driver.first_bind_time+3600
          else data_driver_bef.first_bind_time end,--'初次绑定时间',
          
          nvl(data_driver.total_pay,data_driver_bef.total_pay),--'总计-已打款收入', 
          nvl(data_driver.inviter_role,data_driver_bef.inviter_role),--'', 
          nvl(data_driver.inviter_id,data_driver_bef.inviter_id),--'', 
          nvl(data_driver.block,data_driver_bef.block),--'后台管理司机接单状态(0: 允许 1:不允许)', 
          nvl(data_driver.serv_type,data_driver_bef.serv_type),--'1 专车 2 快车 3 科科车', 
          nvl(data_driver.serv_score,data_driver_bef.serv_score),--'司机服务分', 
          nvl(data_driver.local_gov_ids,data_driver_bef.local_gov_ids),--'行会ID,json', 
          
          nvl(from_unixtime((unix_timestamp(regexp_replace(regexp_replace(data_driver.updated_at,'T',' '),'Z',''))+3600),'yyyy-MM-dd HH:mm:ss'),data_driver_bef.updated_at),--'最后更新时间',
          
          
          nvl(data_driver.fault,data_driver_bef.fault),--'正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5', 
          nvl(data_driver.city_id,data_driver_bef.city_id),--'所属城市ID', 
          nvl(data_driver.language,data_driver_bef.language),--'客户端语言', 
          case when
          data_driver.end_service_time=0 then 0
          when data_driver.end_service_time!=0 and data_driver.end_service_time is not null then data_driver.end_service_time+3600
          else data_driver_bef.end_service_time end,--'专车司机结束收份子钱时间',
          
          
          case when
          data_driver.last_online_time=0 then 0
          when data_driver.last_online_time!=0 and data_driver.last_online_time is not null then data_driver.last_online_time+3600
          else data_driver_bef.last_online_time end,--'最后上线时间', 
          
          
          case when
          data_driver.last_offline_time=0 then 0
          when data_driver.last_offline_time!=0 and data_driver.last_offline_time is not null then data_driver.last_offline_time+3600
          else data_driver_bef.last_offline_time end,--'最后下线时间', 
          
          nvl(data_driver.avoid_highway,data_driver_bef.avoid_highway),--'避开高速 (0: 禁用 1: 启用)', 
          nvl(data_driver.rongcloud_token,data_driver_bef.rongcloud_token),--'融云token', 
          nvl(data_driver.support_carpool,data_driver_bef.support_carpool),--'是否支持拼车', 
          nvl(data_driver.last_trip_id,data_driver_bef.last_trip_id),--'最近一个行程的ID', 
          nvl(data_driver.assign_mode,data_driver_bef.assign_mode),--'强派模式 (0: 禁用 1: 启用)', 
          nvl(data_driver.auto_start,data_driver_bef.auto_start),--'自动开始 (0: 禁用 1: 启用)',

          'nal' as country_code,
          '{pt}' as dt

        FROM
        (select * 
        from oride_dw.dwd_oride_driver_extend_df
        where dt='{bef_yes_day}') data_driver_bef
        full outer join 
        (
            SELECT 
                * 
            FROM
             (
                SELECT 
                    *,
                     row_number() over(partition by t.id order by t.`__ts_ms` desc) as order_by
                FROM oride_dw_ods.ods_binlog_base_data_driver_extend_hi  t
                WHERE concat_ws(' ',dt,hour) BETWEEN '{bef_yes_day} 23' AND '{pt} 22'--取昨天1天数据与今天早上00数据
             ) t1
            where t1.`__deleted` = 'false' and t1.order_by = 1
        ) data_driver
        on data_driver_bef.id=data_driver.id;
        
'''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
        )
    return HQL

# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    select count(1)-count(distinct id) as cnt
    from {db}.{table}
    where dt='{pt}'
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
    _sql = dwd_oride_driver_extend_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_driver_extend_df_task = PythonOperator(
    task_id='dwd_oride_driver_extend_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_binlog_base_data_driver_extend_hi_prev_day_task >> dwd_oride_driver_extend_df_task
dwd_oride_driver_extend_df_prev_day_tesk >> dwd_oride_driver_extend_df_task
