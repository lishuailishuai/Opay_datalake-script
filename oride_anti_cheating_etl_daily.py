from datetime import datetime, timedelta

import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
from airflow.sensors import UFileSensor

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 7, 15),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_anti_cheating_etl_daily',
    schedule_interval="00 05 * * *",
    default_args=args)


dwd_oride_order_location_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_location_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_location_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

clear_order_location_mysql_data = MySqlOperator(
    task_id='clear_order_location_mysql_data',
    sql="""
        DELETE FROM oride_order_location_info WHERE dt='{ds}' or dt <= '{before_15_day}';
    """.format(
        ds='{{ds}}',
        before_15_day ='{{ macros.ds_add(ds, -15) }}'
    ),
    mysql_conn_id='mysql_bi',
    dag=dag)

order_location_info_to_msyql = HiveToMySqlTransfer(
    task_id='order_location_info_to_msyql',
    sql="""
            select 
            null,
            dt,
            order_id  ,
            user_id  ,
            driver_id  ,
            create_time ,
            status ,
            start_loc ,
            end_loc ,
            looking_for_a_driver_show ,
            successful_order_show ,
            accept_order_click ,
            rider_arrive_show ,
            confirm_arrive_click_arrived ,
            pick_up_passengers_sliding_arrived ,
            start_ride_show ,
            start_ride_sliding ,
            complete_the_order_show ,
            start_ride_sliding_arrived ,
            loc_list 
            from oride_dw.dwd_oride_order_location_di
            where country_code = 'nal' and dt='{{ ds }}'

        """,
    mysql_conn_id='mysql_bi',
    mysql_table='oride_order_location_info',
    dag=dag)


dwd_oride_order_location_di_prev_day_task >> clear_order_location_mysql_data >> order_location_info_to_msyql
