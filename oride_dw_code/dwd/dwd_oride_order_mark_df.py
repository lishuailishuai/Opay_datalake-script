# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor

from utils.connection_helper import get_hive_cursor

args = {
    'owner': 'chenlili',
    'start_date': datetime(2019, 8, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_mark_df',
                  schedule_interval="20 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dwd_oride_order_base_include_test_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_mark_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_order_mark_df_task = HiveOperator(
    task_id='dwd_oride_order_mark_df_task',

    hql='''
SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT order_id,  --订单ID
           create_time_s as create_time, --下单时间
           passenger_id,  --乘客ID
           status,--订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)
           2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 as start_distance,--相邻两单起点距离
           2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 as end_distance, --相邻两单终点距离
        IF(status IN (4,5), 1,
            IF(order_id=order_id2, 1,
                IF(ABS(create_time2-create_time)<=1800 AND
                    2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 AND
                    2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
                )
            )
        ) AS valid_mark,  --订单有效标志:1有效 0无效
        'nal' AS country_code,
        '{pt}' AS dt
    FROM (
        SELECT
            order_id,
            passenger_id,
            start_lng,
            start_lat,
            end_lng,
            end_lat,
            unix_timestamp(create_time) as create_time,
            create_time as create_time_s,
            status,
            LEAD(unix_timestamp(create_time),1,unix_timestamp(create_time)) OVER(PARTITION BY passenger_id ORDER BY unix_timestamp(create_time)) create_time2,
            LEAD(start_lng,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) start_lng2,
            LEAD(start_lat,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) start_lat2,
            LEAD(end_lng,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) end_lng2,
            LEAD(end_lat,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) end_lat2,
            LEAD(order_id,1,order_id) OVER(PARTITION BY passenger_id ORDER BY create_time) order_id2
        FROM oride_dw.dwd_oride_order_base_include_test_df
        WHERE dt in ('{pt}','his') AND
            create_date = '{pt}' 
        ) AS t

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',

        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)


def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY order_id HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag)

# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dwd_oride_order_base_include_test_df_prev_day_task >> \
sleep_time >> \
dwd_oride_order_mark_df_task >> \
task_check_key_data >> \
touchz_data_success
