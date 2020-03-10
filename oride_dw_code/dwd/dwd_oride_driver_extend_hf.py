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
    'owner': 'lijialong',
    'start_date': datetime(2020, 3, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_extend_hf',
                  schedule_interval="40 * * * *",
                  default_args=args,
                  )



##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_driver_extend_hf"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_base_data_driver_hi_prev_day_task = OssSensor(
        task_id='ods_binlog_base_data_driver_hi_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={now_hour}/_SUCCESS'.format(
            hdfs_path_str="oride_binlog/oride_db.oride_data.data_driver",
            pt='{{ds}}',
            now_day='{{macros.ds_add(ds, +1)}}' ,
            now_hour='{{ execution_date.strftime("%H") }}'
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

def dwd_oride_driver_extend_hf_sql_task(ds,hour):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        SELECT
          id , -- '司机 ID',
          serv_mode ,-- '服务模式 (0: no service, 1: in service, 99:招手停)',
          serv_status ,-- '服务状态 (0: wait assign, 1: pick up, 2:wait, 3: send, 4:arrive, 5:pick order)',
          order_rate ,-- '接单率',
          assign_order ,-- '派单数量',
          take_order ,-- '接单数量',
          avg_score ,-- '平均评分',
          total_score ,-- '总评分',
          score_times ,-- '评分次数',
          last_order_id ,-- '最近一个订单的ID',
          base.local_register_time as register_time ,-- '注册时间',
          base.local_login_time as login_time ,-- '最后登陆时间',
          is_bind, --, '状态 0 未绑定 1 已绑定',
          base.local_first_bind_time as first_bind_time ,-- '初次绑定时间',
          total_pay, --, '总计-已打款收入',
          inviter_role ,--,
          inviter_id ,--,
          block ,--  '后台管理司机接单状态(0: 允许 1:不允许)',
          serv_type ,-- '1 专车 2 快车 3 科科车',
          serv_score    ,-- '司机服务分',
          local_gov_ids     ,-- '行会ID,json',
          base.updated_time     ,-- '最后更新时间',
          fault     ,-- '正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5',
          city_id   ,-- '所属城市ID',
          language  ,-- '客户端语言',
          base.local_end_service_time as end_service_time ,-- '专车司机结束收份子钱时间',
          base.local_last_online_time as last_online_time ,-- '最后上线时间',
          base.local_last_offline_time as last_offline_time ,-- '最后下线时间',
          avoid_highway ,-- '避开高速 (0: 禁用 1: 启用)',
          rongcloud_token  ,-- '融云token',
          support_carpool ,-- '是否支持拼车',
          last_trip_id ,-- '最近一个行程的ID',
          assign_mode   ,-- '强派模式 (0: 禁用 1: 启用)',
          auto_start      ,-- '自动开始 (0: 禁用 1: 启用)',
          country_id ,-- '所属国家',
          fee_free      ,-- '免佣金（0:不免佣金 1:免佣金）',
          version     ,-- '司机端版本号',
          home_confirm ,-- '顺路地址是否确认 (0: 未确认 1: 已确认)',
          home_address   ,-- '顺路地址'
          home_status ,--'顺路地址状态 (0: 关闭 1: 开启)',
          home_lng  ,-- '顺路地址经度',
          home_lat  ,-- '顺路地址纬度',
          level     ,-- '司机等级',
          'nal' as country_code,
         '{pt}' as dt
    FROM
    (
     select
        *
     from 
     (  
        select
            *,
             if(t.register_time=0,0,(t.register_time + 1 * 60 * 60)) as local_register_time,
             if(t.login_time=0,0,(t.login_time + 1 * 60 * 60)) as local_login_time ,
             if(t.first_bind_time=0,0,(t.first_bind_time + 1 * 60 * 60)) as local_first_bind_time ,
             if(t.end_service_time=0,0,(t.end_service_time + 1 * 60 * 60)) as local_end_service_time ,
             if(t.last_online_time=0,0,(t.last_online_time + 1 * 60 * 60)) as local_last_online_time ,
             if(t.last_offline_time=0,0,(t.last_offline_time + 1 * 60 * 60)) as local_last_offline_time ,
             from_unixtime((unix_timestamp(regexp_replace(regexp_replace(t.updated_at,'T',' '),'Z',''))+3600),'yyyy-MM-dd HH:mm:ss') as updated_time,
             row_number() over(partition by t.id order by t.`__ts_ms` desc,t.`__file` desc,cast(t.`__pos` as int) desc) as order_by
             
        FROM oride_dw_ods.ods_binlog_base_data_driver_extend_h_his  t

        WHERE  dt='{pt}' and hour='{now_hour}'
        ) t1
    where t1.`__deleted` = 'false' and t1.order_by = 1
) base;
            
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
    _sql = dwd_oride_driver_extend_hf_sql_task(ds,v_hour)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "false",v_hour)


dwd_oride_driver_extend_hf_task = PythonOperator(
    task_id='dwd_oride_driver_extend_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_binlog_base_data_driver_hi_prev_day_task >>  dwd_oride_driver_extend_hf_task
