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


dependence_dwd_oride_order_location_event_hi_prev_day_task = HivePartitionSensor(
    task_id="dwd_oride_order_location_event_hi_prev_day_task",
    table="dwd_oride_client_event_detail_hi",
    partition="""dt='{{ ds }}' and hour='23'""",
    schema="oride_dw",
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
            (
                select
                event_name,
                event_time ,
                get_json_object(event_value,'$.order_id') order_id,
                get_json_object(event_value,'$.lat') lat,
                get_json_object(event_value,'$.lng') lng
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
            ) t where t.order_id is not null
        ),



        middle_data_1 as (
            select 
            od.*,
            nvl(l.loc,'') looking_for_a_driver_show,
            nvl(s.loc,'') successful_order_show,
            nvl(a.loc,'') accept_order_click,
            nvl(r.loc,'') rider_arrive_show,
            nvl(c.loc,'') confirm_arrive_click_arrived

            from order_data od 
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'looking_for_a_driver_show'
            )  l on od.order_id = l.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'successful_order_show'
            )  s on od.order_id = s.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'accept_order_click'
            )  a on od.order_id = a.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'rider_arrive_show'
            )  r on od.order_id = r.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'confirm_arrive_click_arrived'
            ) c on od.order_id = c.order_id
        ),


        middle_data_2 as (
            select 
            m.*,
            nvl(p.loc,'') pick_up_passengers_sliding_arrived,
            nvl(s.loc,'') start_ride_show,
            nvl(st.loc,'') start_ride_sliding,
            nvl(c.loc,'') complete_the_order_show,
            nvl(sta.loc,'') start_ride_sliding_arrived

            from 
            middle_data_1 m 
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'pick_up_passengers_sliding_arrived'
            ) p on m.order_id = p.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_show'
            ) s on m.order_id = s.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_sliding'
            ) st on m.order_id = st.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'complete_the_order_show'
            ) c on m.order_id = c.order_id

            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_sliding_arrived'
            ) sta on m.order_id = sta.order_id
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

dependence_dwd_oride_order_location_event_hi_prev_day_task >> \
dependence_dwd_oride_driver_location_event_hi_prev_day_task >> \
dependence_dwd_oride_passanger_location_event_hi_prev_day_task >> \
sleep_time >> dwd_oride_order_location_di_task >> task_check_key_data >> touchz_data_success
