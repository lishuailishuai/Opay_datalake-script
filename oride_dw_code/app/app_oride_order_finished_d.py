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

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 10, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_finished_d',
                  schedule_interval="30 4 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag
)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_oride_driver_timerange_di_task = UFileSensor(
    task_id='dwd_oride_driver_timerange_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_timerange_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dim_oride_city_task = HivePartitionSensor(
    task_id="dim_oride_city_task",
    table="dim_oride_city",
    partition="dt='{{ ds }}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


dwd_oride_driver_extend_df_task = UFileSensor(
    task_id='dwd_oride_driver_extend_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_extend_df",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dwd_oride_order_base_include_test_di_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_order_finished_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

app_oride_order_finished_d_task = HiveOperator(
    task_id='app_oride_order_finished_d_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.{table} partition(dt)
            select k4.city_name, --城市名称
	            nvl(k4.wdl,0) wdl, --完单量
	            count(distinct k4.id) qss,    --司机数量
	            round(sum(k4.every_day_driver_online_dur)/(1000*3600*count(distinct k4.id)),2) as avg_online_dur,--人均在线时长(小时)
	            nvl(k4.dt,date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)) dt
            from
            (
	            select k1.id, --k1.id是driverID
		            k3.city_name,
		            k2.dt,
		            k2.wdl,
		            k2.every_day_driver_online_dur
	            from oride_dw.dwd_oride_driver_extend_df k1
	            left join oride_dw.dim_oride_city k3	
	            on k1.city_id=k3.city_id
	            left join
	            (
	                select ord.dt,
	                    ord.driver_id,
	                    ord.wdl,
	                    if(ord.is_td_finish>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_finish_billing_dur,0) + nvl(ord.td_cannel_pick_dur,0),0) as every_day_driver_online_dur
	                from
	                (
	                    select from_unixtime(a.create_time,'yyyy-MM-dd') dt,
			                a.driver_id,
			                count(distinct case when a.status in (4,5) then a.order_id end) wdl,
			                sum(a.td_cannel_pick_dur) as td_cannel_pick_dur,
			                sum(a.is_td_finish) as is_td_finish,  --用于判断该订单是否是完单
                            sum(if(a.is_td_finish = 1,a.td_finish_billing_dur,0)) as td_finish_billing_dur
		                from oride_dw.dwd_oride_order_base_include_test_di a		
		                where from_unixtime(a.create_time,'yyyy-MM-dd')
			                between date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) 
			                and date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)
		                group by from_unixtime(a.create_time,'yyyy-MM-dd'),a.driver_id
		            ) ord 
                    LEFT OUTER JOIN
                    (
                        SELECT *
                        FROM oride_dw.dwd_oride_driver_timerange_di
                        WHERE  dt='{pt}'
                    ) dtr 
                    ON ord.driver_id=dtr.driver_id
	            ) k2
	            on k1.id=k2.driver_id
	            where k1.fault=0
            ) k4
            group by k4.city_name,
            nvl(k4.dt,date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)),
            nvl(k4.wdl,0);
    '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)

# 生成_SUCCESS
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dwd_oride_driver_timerange_di_task >> dim_oride_city_task >> dwd_oride_driver_extend_df_task \
>> dwd_oride_order_base_include_test_di_task >> sleep_time >>app_oride_order_finished_d_task \
>> touchz_data_success