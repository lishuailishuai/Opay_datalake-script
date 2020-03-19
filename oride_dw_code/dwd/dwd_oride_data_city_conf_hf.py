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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_data_city_conf_hf',
                  schedule_interval="23 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_data_city_conf_hf"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_base_data_city_conf_h_his_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_city_conf_h_his_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={now_hour}/_SUCCESS'.format(
        hdfs_path_str="oride_h_his/ods_binlog_base_data_city_conf_h_his",
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
         "partition": "dt={pt}/hour={hour}".format(pt=ds, hour=execution_date.strftime("%H")),
         "timeout": "400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_data_city_conf_hf_sql_task(ds, hour):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(dt,hour)

        SELECT id, -- 城市 ID
            name, -- 城市名称
            country, -- 国家
            shape, -- 形状：1 圆形, 2 多边形
            area, -- 区域设置数据
            serv_type, -- 开启的服务类型[1,2,99] 1 专车 2 快车 99 招手停
            avoid_highway_type, -- 可设置避开高速的服务类型[1,2] 1 专车 2 快车
            VALIDATE, -- 本条数据是否有效 0 无效，1 有效
            opening_time, -- 开启时间
            assign_type, -- 强派的服务类型[1,2,3] 1 专车 2 快车 3 keke车
            allow_flagdown_type, -- 允许招手停的服务类型
            country_id, -- 国家ID
            recruit_serv_type, -- 招募后台服务类型展示使用
            serv_time, -- 车型运行时间
            merge_switch, -- 合并专快开关0.关闭 1.开'  
             '{pt}' AS dt, 
             hour 
            FROM 
              (SELECT * 
               FROM 
                 (SELECT *, 
                         row_number() over(partition BY t.id 
                                           ORDER BY t.`__ts_ms` DESC,t.`__file` DESC,cast(t.`__pos` AS int) DESC) AS order_by 
                  FROM oride_dw_ods.ods_binlog_base_data_city_conf_h_his t 
                  WHERE dt='{pt}' 
                    AND hour='{now_hour}' ) t1 
               WHERE t1.`__deleted` = 'false'
                 AND t1.order_by = 1 ) base;
'''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        now_hour=hour,
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 所有国家是否上线。false 没有
        第二个参数true: 数据目录是有country_code分区。false 没有
        第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "false", "true",v_hour)

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_data_city_conf_hf_sql_task(ds,v_hour)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


dwd_oride_data_city_conf_hf_task = PythonOperator(
    task_id='dwd_oride_data_city_conf_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_binlog_base_data_city_conf_h_his_prev_day_task >> dwd_oride_data_city_conf_hf_task
