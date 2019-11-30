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
    'owner': 'lijialong',
    'start_date': datetime(2019, 9, 21),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_skyeye_analysis_d',
                  schedule_interval="40 8 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_skyeye_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_skyeye_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_skyeye_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


dependence_dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_order_skyeye_analysis_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

##----------------------------------------- 脚本 ---------------------------------------##


app_oride_order_skyeye_analysis_d_task = HiveOperator(
    task_id='app_oride_order_skyeye_analysis_d_task',
    hql='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)
    SELECT 
        a.ver, --
        a.order_id,--订单id
        a.tag_id,
        b.driver_id,--司机id
        b.passenger_id  as user_id,--乘客id
        b.duration,--订单持续时间
        b.distance,--订单距离
        b.price,--订单价格
        b.reward,--司机奖励
        b.city_id,--城市id
        b.pay_mode as mode,--支付模式
        c.phone_number,--司机手机号
        'nal' as country_code,--国家码
        '{pt}'  as dt
        FROM
    (
        SELECT *  
            FROM  oride_dw.dwd_oride_order_skyeye_di 
            WHERE dt = '{pt}' and ver is not null
    ) a
    LEFT JOIN
    (   SELECT 
            order_id,
            driver_id,
            passenger_id,
            duration,
            distance,
            price,
            reward,
            city_id,
            pay_mode,
            country_code
       FROM oride_dw.dwd_oride_order_base_include_test_di
       WHERE dt = '{pt}'
         AND from_unixtime(create_time,"yyyy-MM-dd") = '{pt}'
    ) b ON a.order_id = b.order_id
    LEFT JOIN
    (
        SELECT
            driver_id,
            phone_number
        FROM oride_dw.dim_oride_driver_base 
        WHERE dt = '{pt}'
    ) c ON b.driver_id = c.driver_id
'''.format(
        pt='{{ds}}',
        table=table_name
    ),
    schema='oride_dw',
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

dependence_dwd_oride_order_skyeye_di_prev_day_task >> \
dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dim_oride_driver_base_prev_day_task >> \
sleep_time >> app_oride_order_skyeye_analysis_d_task >> touchz_data_success
