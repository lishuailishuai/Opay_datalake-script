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
    'start_date': datetime(2020, 3, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_user_extend_hf',
                  schedule_interval="40 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_user_extend_hf"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_base_data_user_extend_h_his_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_user_extend_h_his_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={now_hour}/_SUCCESS'.format(
        hdfs_path_str="oride_h_his/ods_binlog_base_data_user_extend_h_his",
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        now_hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, execution_date, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}/hour={hour}".format(pt=ds, hour=execution_date.strftime("%H")),
         "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_user_extend_hf_sql_task(ds, hour):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt,hour)

          select 
          id,--'用户 ID',
          take_order,--'接单数量',
          avg_score,--'平均评分',
          total_score,--'总评分',
          score_times,--'评分次数',
          bonus,--'奖励金',
          balance,--COMMENT '余额',
          last_order_id,--'最近一个订单的ID',
          if(register_time=0,0,(register_time + 1 * 60 * 60)) as local_register_time,--'注册时间',
          if(login_time=0,0,(login_time + 1 * 60 * 60)) as local_login_time,--'最后登陆时间',
          inviter_role,--'',
          inviter_id,--'',
          invite_num,--'',
          invite_complete_num,--'',
          invite_award,--'',
          from_unixtime(unix_timestamp(updated_at)+3600,'yyyy-MM-dd HH:mm:ss'),--'最后更新时间',
          pay_type,--'user auto pay settings(-1: not set 0: manual payment 1: auto payment)',
          city_id,--'注册城市',
          language,--'客户端语言',
          finish_order,--'完单数量',
          mark,--'按位通用标记',
          country_id,--所属国家',
          gender,--'性别:0.未设置 1.男 2.女',
          version,--'乘客端版本号',
          protocol_no,--'签约opay 免密支付协议号',
          if(protocol_time=0,0,(protocol_time + 1 * 60 * 60)) as local_protocol_time,--'签约/解约时间'
          'nal' as country_code,
          '{pt}' as dt,
          hour
          from
          (
          select 
          *
          from
          (
          select 
              *,
    row_number() over(partition by t.id order by t.`__ts_ms` desc,t.`__file` desc,cast(t.`__pos` as int) desc) as order_by
          from
          oride_dw_ods.ods_binlog_base_data_user_extend_h_his t
          WHERE  dt='{pt}' and hour='{now_hour}'
          ) t1
          where t1.`__deleted` = 'false' and t1.order_by = 1
          ) t2;


'''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        now_hour=hour,
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    hive_hook = HiveCliHook()

    v_hour = kwargs.get('v_execution_hour')

    # 读取sql
    _sql = dwd_oride_user_extend_hf_sql_task(ds, v_hour)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "false", v_hour)


dwd_oride_user_extend_hf_task = PythonOperator(
    task_id='dwd_oride_user_extend_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_binlog_base_data_user_extend_h_his_prev_day_task >> dwd_oride_user_extend_hf_task
