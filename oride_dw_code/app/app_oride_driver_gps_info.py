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
from plugins.CountriesPublicFrame import CountriesPublicFrame
from plugins.TaskHourSuccessCountMonitor import TaskHourSuccessCountMonitor
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 1, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_gps_info',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_gps_info"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
moto_locations_prev_day_task = HivePartitionSensor(
    task_id="moto_locations_prev_day_task",
    table="moto_locations",
    partition="dt='{{ds}}'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_oride_assets_sku_df_prev_day_task = OssSensor(
        task_id='ods_sqoop_base_oride_assets_sku_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/opay_assets/oride_assets_sku",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_gps_info_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(dt='{pt}')
    -- 每天凌晨2点读取所有车辆的停靠点，只取top 3 --请做成计划任务
    SELECT driver_id,plate_number,chassis,gps_id,loc,times,type FROM 
    (
    SELECT oas.driver_id,
           oas.plate_number,
           oas.chassis,
           ml.gps_id,
           ml.loc,
           ml.times,
           '0' AS type,
           row_number()over(partition by ml.gps_id order by ml.times desc) row_num       
    FROM
      (SELECT gps_id,
              CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)) AS loc,
              COUNT(CONCAT(ROUND(latitude,4),",",ROUND(longitude,4))) AS times
       FROM oride_source.moto_locations
       WHERE dt<=date_add(current_date(),-1)
         AND dt>=date_add(current_date(),-16)
         AND hour IN ('02',
                      '03')
       GROUP BY gps_id,
                CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)) HAVING COUNT(CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)))>5) ml
    LEFT JOIN
      (SELECT claimed_driver_id as driver_id,
              property_id,
              get_json_object(custom_attribute_value, "$.PLATE_NUMBER") AS plate_number,
              get_json_object(custom_attribute_value, "$.CHASSIS_NUMBER") AS chassis,
              get_json_object(custom_attribute_value, "$.GPS_IMEI") AS gps_imei
       FROM oride_dw_ods.ods_sqoop_base_oride_assets_sku_df
       WHERE dt='{pt}'
         AND business_id=1
         AND property_id=14
         AND status<>99
         AND claimed_driver_id>0
         AND get_json_object(custom_attribute_value, "$.GPS_IMEI")<>'') oas ON oas.gps_imei = ml.gps_id
    WHERE ISNULL(oas.gps_imei)=0
    ORDER BY ml.gps_id,
             ml.times DESC
    ) gpslist
    WHERE row_num<=3
    
    union all
    
    -- 每天凌晨8-20点读取所有车辆的停靠点，只取 top 3 --请做成计划任务
    SELECT driver_id,plate_number,chassis,gps_id,loc,times,type FROM 
    (
    SELECT oas.driver_id,
           oas.plate_number,
           oas.chassis,
           ml.gps_id,
           ml.loc,
           ml.times,
           '1' AS type,
           row_number()over(partition by ml.gps_id order by ml.times desc) row_num       
    FROM
      (SELECT gps_id,
              CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)) AS loc,
              COUNT(CONCAT(ROUND(latitude,4),",",ROUND(longitude,4))) AS times
       FROM oride_source.moto_locations
       WHERE dt<=date_add(current_date(),-1)
         AND dt>=date_add(current_date(),-7)
         AND hour>='08'
         AND hour<='20'
       GROUP BY gps_id,
                CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)) HAVING COUNT(CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)))>5) ml
    LEFT JOIN
      (SELECT claimed_driver_id AS driver_id,
              property_id,
              get_json_object(custom_attribute_value, "$.PLATE_NUMBER") AS plate_number,
              get_json_object(custom_attribute_value, "$.CHASSIS_NUMBER") AS chassis,
              get_json_object(custom_attribute_value, "$.GPS_IMEI") AS gps_imei
       FROM oride_dw_ods.ods_sqoop_base_oride_assets_sku_df
       WHERE dt='{pt}'
         AND business_id=1
         AND property_id=14
         AND status<>99
         AND claimed_driver_id>0
         AND get_json_object(custom_attribute_value, "$.GPS_IMEI")<>'') oas ON oas.gps_imei = ml.gps_id
    WHERE ISNULL(oas.gps_imei)=0
    ORDER BY ml.gps_id,
             ml.times DESC
    ) gpslist
    WHERE row_num<=3
    
    union all
    
    -- 取每个 GPS_IMEI 失联前60分钟 TOP 1 的点 --请做成计划任务
    SELECT driver_id,plate_number,chassis,gps_id,loc,times,type 
    FROM
      (SELECT oas.driver_id,
              oas.plate_number,
              oas.chassis,
              gpsdev.gps_id,
              gpsdev.loc,
              gpsdev.times,
              '2' AS TYPE,
              row_number()over(partition BY gpsdev.gps_id
                               ORDER BY gpsdev.times DESC) row_num
       FROM
         (SELECT ml.gps_id,
                 CONCAT(ROUND(latitude,4),",",ROUND(longitude,4)) AS loc,
                 COUNT(CONCAT(ROUND(latitude,4),",",ROUND(longitude,4))) AS times
          FROM oride_source.moto_locations ml
          LEFT JOIN
            (SELECT gps_id,
                    MAX(times) AS lasttime
             FROM oride_source.moto_locations
             WHERE dt<=date_add(current_date(),-1)
               AND dt>=date_add(current_date(),-7)
               AND ISNULL(gps_type)=0
               AND ISNULL(gps_id)=0
             GROUP BY gps_id) lastml ON lastml.gps_id = ml.gps_id
          WHERE ISNULL(lastml.gps_id)=0
            AND dt<=date_add(current_date(),-1)
            AND dt>=date_add(current_date(),-7)
            AND ml.times>=lastml.lasttime-60*1000
          GROUP BY ml.gps_id,
                   CONCAT(ROUND(latitude,4),",",ROUND(longitude,4))) gpsdev
       LEFT JOIN
         (SELECT claimed_driver_id AS driver_id,
                 property_id,
                 get_json_object(custom_attribute_value, "$.PLATE_NUMBER") AS plate_number,
                 get_json_object(custom_attribute_value, "$.CHASSIS_NUMBER") AS chassis,
                 get_json_object(custom_attribute_value, "$.GPS_IMEI") AS gps_imei
          FROM oride_dw_ods.ods_sqoop_base_oride_assets_sku_df
          WHERE dt='{pt}'
            AND business_id=1
            AND property_id=14
            AND status<>99
            AND claimed_driver_id>0
            AND get_json_object(custom_attribute_value, "$.GPS_IMEI")<>'') oas ON oas.gps_imei = gpsdev.gps_id
       WHERE ISNULL(oas.gps_imei)=0
       ORDER BY gpsdev.gps_id,
                gpsdev.times DESC
      ) gpslist
    WHERE row_num=1;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
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
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "false", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_driver_gps_info_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_driver_gps_info_task = PythonOperator(
    task_id='app_oride_driver_gps_info_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

moto_locations_prev_day_task >> app_oride_driver_gps_info_task
ods_sqoop_base_oride_assets_sku_df_prev_day_task >> app_oride_driver_gps_info_task

