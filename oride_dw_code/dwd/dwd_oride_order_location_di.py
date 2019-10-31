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
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 10, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_location_di',
                  schedule_interval="30 4 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_client_event_detail_hi_prev_day_task = UFileSensor(
    task_id='dependence_dwd_oride_client_event_detail_hi_prev_day_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
        pt='{{ds}}',
        hour='23'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# 依赖前一天分区
dependence_dwd_oride_driver_location_event_hi_prev_day_task = HivePartitionSensor(
    task_id="dwd_oride_driver_location_event_hi_prev_day_task",
    table="ods_log_driver_track_data_hi",
    partition="""dt='{{ ds }}' and hour='23'""",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_passanger_location_event_hi_prev_day_task = HivePartitionSensor(
    task_id="dwd_oride_passanger_location_event_hi_prev_day_task",
    table="ods_sqoop_base_data_order_df",
    partition="""dt='{{ ds }}'""",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_order_location_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name



##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_order_location_di_task = HiveOperator(
    task_id='dwd_oride_order_location_di_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        with order_data as (
            select 
            id as order_id,
            user_id,
            driver_id,
            create_time,
            status,
            concat(start_lat,'_',start_lng) start_loc,
            concat(end_lat,'_',end_lng) end_loc
            from oride_dw_ods.ods_sqoop_base_data_order_df
            where dt = '{pt}'
            and from_unixtime(create_time,'yyyy-MM-dd') = '{pt}'
            and (status = 4 or status = 5)
        ),


        event_loc_data as (
            select 
            t.event_name event_name,
            t.order_id order_id,
            concat(substring(cast(t.event_time as string),0,10),'_',t.lat,'_',t.lng) loc
            from 
            (   select 
                s.event_name,
                s.event_time,
                s.order_id,
                s.lat,
                s.lng
                
                from 
                (
                    select
                    event_name,
                    event_time ,
                    get_json_object(event_value,'$.order_id') order_id,
                    get_json_object(event_value,'$.lat') lat,
                    get_json_object(event_value,'$.lng') lng,
                    row_number() over(partition by event_name,get_json_object(event_value,'$.order_id') order by event_time) order_by
                    from oride_dw.dwd_oride_client_event_detail_hi
                    where dt = '{pt}'
                    and event_name in (
                        'looking_for_a_driver_show',
                        'successful_order_show',
                        'accept_order_click',
                        'rider_arrive_show',
                        'confirm_arrive_click_arrived',
                        'pick_up_passengers_sliding_arrived',
                        'start_ride_show',
                        'start_ride_sliding',
                        'complete_the_order_show',
                        'start_ride_sliding_arrived'
                    ) 
                    and get_json_object(event_value,'$.order_id') is not null
                ) s where s.order_by = 1
            ) t 
        ),

        middle_data_1 as (
            select 
            od.*,
            nvl(if(l.event_name = 'looking_for_a_driver_show',l.loc,''),'') looking_for_a_driver_show,
            nvl(if(l.event_name = 'successful_order_show',l.loc,''),'') successful_order_show,
            nvl(if(l.event_name = 'accept_order_click',l.loc,''),'') accept_order_click,
            nvl(if(l.event_name = 'rider_arrive_show',l.loc,''),'') rider_arrive_show,
            nvl(if(l.event_name = 'confirm_arrive_click_arrived',l.loc,''),'') confirm_arrive_click_arrived,
            nvl(if(l.event_name = 'pick_up_passengers_sliding_arrived',l.loc,''),'') pick_up_passengers_sliding_arrived,
            nvl(if(l.event_name = 'start_ride_show',l.loc,''),'') start_ride_show,
            nvl(if(l.event_name = 'start_ride_sliding',l.loc,''),'') start_ride_sliding,
            nvl(if(l.event_name = 'complete_the_order_show',l.loc,''),'') complete_the_order_show,
            nvl(if(l.event_name = 'start_ride_sliding_arrived',l.loc,''),'') start_ride_sliding_arrived
            
            from order_data od 
            left join (
                select 
                order_id,
                event_name,
                loc 
                from event_loc_data 
            )  l on od.order_id = l.order_id

        ),

        middle_data_2 as (
            select 
            m.order_id,
            m.user_id,
            m.driver_id,
            m.create_time,
            m.status,
            m.start_loc,
            m.end_loc,
            concat_ws('',collect_list(looking_for_a_driver_show)) looking_for_a_driver_show,
            concat_ws('',collect_list(successful_order_show)) successful_order_show,
            concat_ws('',collect_list(accept_order_click)) accept_order_click,
            concat_ws('',collect_list(rider_arrive_show)) rider_arrive_show,
            concat_ws('',collect_list(confirm_arrive_click_arrived)) confirm_arrive_click_arrived,
            concat_ws('',collect_list(pick_up_passengers_sliding_arrived)) pick_up_passengers_sliding_arrived,
            concat_ws('',collect_list(start_ride_show)) start_ride_show,
            concat_ws('',collect_list(start_ride_sliding)) start_ride_sliding,
            concat_ws('',collect_list(complete_the_order_show)) complete_the_order_show,
            concat_ws('',collect_list(start_ride_sliding_arrived)) start_ride_sliding_arrived

            from 
            middle_data_1 m 
            group by 
            m.order_id,
            m.user_id,
            m.driver_id,
            m.create_time,
            m.status,
            m.start_loc,
            m.end_loc

        ),


        
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw_ods.ods_log_driver_track_data_hi 
            where dt = '{pt}'
            and order_id <> 0
            group by order_id
        )
        
        
        insert overwrite table oride_dw.dwd_oride_order_location_di partition(country_code,dt)
        
        select
        m.order_id,
        m.user_id,
        m.driver_id,
        m.create_time,
        m.status,
        m.start_loc,
        m.end_loc,
        m.looking_for_a_driver_show,
        m.successful_order_show,
        m.accept_order_click,
        m.rider_arrive_show,
        m.confirm_arrive_click_arrived,
        m.pick_up_passengers_sliding_arrived,
        m.start_ride_show,
        m.start_ride_sliding,
        m.complete_the_order_show,
        m.start_ride_sliding_arrived,
        m.loc_list,
        m.country_code as country_code,
        m.dt as dt
        from 
        (
            select 
            row_number() over(partition by m.order_id ORDER BY m.create_time DESC) id,
            m.order_id,
            m.user_id,
            m.driver_id,
            m.create_time,
            m.status,
            m.start_loc,
            m.end_loc,
            m.looking_for_a_driver_show,
            m.successful_order_show,
            m.accept_order_click,
            m.rider_arrive_show,
            m.confirm_arrive_click_arrived,
            m.pick_up_passengers_sliding_arrived,
            m.start_ride_show,
            m.start_ride_sliding,
            m.complete_the_order_show,
            m.start_ride_sliding_arrived,
            m.loc_list,
            m.country_code,
            m.dt 
            from 
            (
                select 
                m.*,
                nvl(d.loc_list,'') as loc_list,
                'nal' as country_code,
                '{pt}' as dt
                from middle_data_2 m 
                left join 
                driver_location d on m.order_id = d.order_id
            ) m 
        ) m 
        where m.id = 1
        
        ;

'''.format(
        pt='{{ds}}',
        now_day='{{ds}}',
        table=table_name
    ),
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
      GROUP BY 
      order_id
      HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=ds,
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
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_client_event_detail_hi_prev_day_task >> \
dependence_dwd_oride_driver_location_event_hi_prev_day_task >> \
dependence_dwd_oride_passanger_location_event_hi_prev_day_task >> \
sleep_time >> dwd_oride_order_location_di_task >> task_check_key_data >> touchz_data_success
