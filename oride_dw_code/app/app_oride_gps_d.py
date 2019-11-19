# -*- coding: utf-8 -*-
"""
昨日司机完单分布图多城市对比
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_gps_d',
                  schedule_interval="00 4 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

moto_locations_task = HivePartitionSensor(
    task_id="moto_locations_task",
    table="moto_locations",
    partition="dt='{{ds}}'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_gps_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

def app_oride_gps_d_sql_task(ds):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert into table {db}.{table} partition(country_code,dt)
            
        select a.gps_type,a.gps_id,a.latitude,a.longitude,a.times,a.hour,0 as lost,
            'nal' as country_code,'{pt}' as dt 
        from(
            SELECT gps_type,gps_id,latitude,longitude,times,hour
            FROM oride_source.moto_locations 
            WHERE hour='02' AND dt='{pt}' 
            group by gps_type,gps_id,latitude,longitude,times,hour
            ORDER BY gps_id,times ASC
        ) as a
        UNION
        select b.gps_type,b.gps_id,b.latitude,b.longitude,b.times,b.hour,1 as lost,
            'nal' as country_code,'{pt}' as dt 
        from(
            SELECT gps_type,tday.gps_id,latitude,longitude,tday.times,hour
            FROM oride_source.moto_locations as tday 
            RIGHT JOIN 
            (
                SELECT  gps_id,max(times) as mtimes 
                FROM  oride_source.moto_locations as subtb1 
                WHERE dt='{yes_day}' 
                GROUP BY gps_id
            ) AS yday ON tday.times = yday.mtimes
            WHERE tday.dt='{yes_day}' 
            AND tday.gps_id NOT IN (SELECT gps_id FROM oride_source.moto_locations as subtb2 WHERE dt='{pt}' GROUP BY subtb2.gps_id)
            group by gps_type,tday.gps_id,latitude,longitude,tday.times,hour
            ORDER BY tday.gps_id,tday.times ASC
        )as b; 
    '''.format(
        pt=ds,
        yes_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
    )
    return HQL



# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_gps_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_gps_d_task = PythonOperator(
    task_id='app_oride_gps_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

moto_locations_task >> app_oride_gps_d_task