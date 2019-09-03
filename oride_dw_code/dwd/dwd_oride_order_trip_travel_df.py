# -*- coding: utf-8 -*-
"""
oride data_trip数据清洗
"""
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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 9, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'dwd_oride_order_trip_travel_df',
    schedule_interval="30 02 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag
)

hive_ods_table = 'ods_sqoop_base_data_trip_df'
hive_ods_db = 'oride_dw'
"""
/------------------------------- 依赖数据源 --------------------------------/
"""
dependence_ods_sqoop_base_data_trip_df = HivePartitionSensor(
    task_id='dependence_ods_sqoop_base_data_trip_df',
    table=hive_ods_table,
    partition="dt='{{ds}}'",
    schema=hive_ods_db,
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
"""
/---------------------------------- end ----------------------------------/
"""

hive_dwd_table = 'oride_dw.dwd_oride_order_trip_travel_df'

# 创建dwd表
create_dwd_table_task = HiveOperator(
    task_id='create_dwd_table_task',
    hql='''
        CREATE EXTERNAL TABLE IF NOT EXISTS {table} (
            travel_id           bigint          comment '行程ID',
            driver_id           bigint          comment '司机ID',
            city_id             bigint          comment '所属城市',
            product_id           int             comment '订单车辆类型(1:driect 2:street 3:keke)',
            pax_num             int             comment '乘客数量',
            pax_max             int             comment '乘客上限',
            duration            bigint          comment '时间',
            distance            bigint          comment '订单距离',
            price               decimal(38,2)   comment '订单价格',
            reward              decimal(38,2)   comment '司机奖励',
            tip                 decimal(38,2)   comment '小费',
            order_id            bigint          comment '订单号',
            create_time         string          comment '创建时间',
            start_time          bigint          comment '开始时间',
            finish_time         bigint          comment '完成时间',
            cancel_time         bigint          comment '取消时间',
            status              int             comment '订单状态 (0: initial, 1: ongoing, 2: finished 3: cancel)',
            pickup_order_id     bigint          comment '接单',
            count_down          bigint          comment '倒计时秒'
        ) 
        PARTITIONED BY (
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/dwd_oride_order_trip_travel_df' 
        TBLPROPERTIES("orc.compress"="SNAPPY") 
    '''.format(table=hive_dwd_table),
    schema='oride_dw',
    dag=dag
)

# 清洗数据
cleaning_data_to_dwd_task = HiveOperator(
    task_id='cleaning_data_to_dwd_task',
    hql='''
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE {table} PARTITION(country_code, dt) 
        SELECT 
            NVL(id, 0) as travel_id,
            NVL(driver_id, 0) as driver_id,
            NVL(city_id, 0) as city_id,
            NVL(serv_type, 0) as product_id,
            NVL(pax_num, 0) as pax_num,
            NVL(pax_max, 0) as pax_max,
            NVL(duration, 0) as duration,
            NVL(distance, 0) as distance,
            (CASE WHEN price IS NULL THEN 0 ELSE price END) as  price,
            (CASE WHEN reward IS NULL THEN 0 ELSE reward END) as  reward,
            (CASE WHEN tip IS NULL THEN 0 ELSE tip END) as  tip,
            NVL(CAST(order_id AS bigint), 0) as order_id,
            from_unixtime(create_time,'yyyy-MM-dd hh:mm:ss') as create_time,
            NVL(start_time, 0) as start_time,
            NVL(finish_time, 0) as finish_time,
            NVL(cancel_time, 0) as cancel_time,
            NVL(status, 0) as status,
            NVL(pickup_order_id, 0) as pickup_order_id,
            NVL(count_down, 0) as count_down, 
            'nal' AS country_code, 
            dt 
        FROM (SELECT 
                *,
                split(replace(replace(order_ids,'[',''),']',''), ',') AS orders 
            FROM {ods_db}.{ods_table} 
            WHERE dt = '{pt}'
            ) AS t 
            LATERAL VIEW posexplode(orders) d AS pos, order_id 
    '''.format(
        table=hive_dwd_table,
        ods_db=hive_ods_db,
        ods_table=hive_ods_table,
        pt='{{ ds }}'
    ),
    schema='oride_dw',
    dag=dag
)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct travel_id,order_id) as cnt
      FROM oride_dw.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    
 
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag
)

# 生成_SUCCESS
touchz_data_success = BashOperator(
    task_id='touchz_data_success',
    bash_command="""
        line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

        if [ $line_num -eq 0 ]; then
            echo "FATAL {hdfs_data_dir} is empty"
            exit 1
        else
            echo "DATA EXPORT Successed ......"
            $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
        fi
    """.format(
        pt='{{ ds }}',
        now_day='{{ macros.ds_add(ds, +1) }}',
        hdfs_data_dir='ufile://opay-datalake/oride/oride_dw/dwd_oride_order_trip_travel_df/country_code=nal/dt={{ ds }}'
    ),
    dag=dag
)


dependence_ods_sqoop_base_data_trip_df >>create_dwd_table_task>>sleep_time >> cleaning_data_to_dwd_task>>task_check_key_data >> touchz_data_success
