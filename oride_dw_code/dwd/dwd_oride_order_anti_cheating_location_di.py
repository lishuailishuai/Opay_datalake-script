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
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.sensors import UFileSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor

args = {
    'owner': 'linan',
    'start_date': datetime(2020, 2, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_anti_cheating_location_di',
                  schedule_interval="30 4 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_anti_cheating_location_di"

##----------------------------------------- 依赖 ---------------------------------------##

dependence_dwd_oride_client_event_detail_hi_prev_day_task = OssSensor(
    task_id='dependence_dwd_oride_client_event_detail_hi_prev_day_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
        pt='{{ds}}',
        hour='23'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_ods_log_driver_track_data_hi_prev_day_task = HivePartitionSensor(
    task_id="ods_log_driver_track_data_hi_prev_day_task",
    table="ods_log_driver_track_data_hi",
    partition="""dt='{{ ds }}' and hour='23'""",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# 依赖前一天分区
dependence_ods_log_user_track_data_hi_prev_day_task = HivePartitionSensor(
    task_id="ods_log_user_track_data_hi_prev_day_task",
    table="ods_log_user_track_data_hi",
    partition="""dt='{{ ds }}' and hour='23'""",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



dependence_dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": db_name, "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##


def dwd_oride_order_anti_cheating_location_di_sql_task(ds):
    HQL = '''

        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict; 

        WITH order_data AS (

        SELECT  ord.order_id               AS order_id 
               ,ord.user_id                AS user_id 
               ,ord.driver_id              AS driver_id 
               ,ord.create_time            AS create_time 
               ,ord.status                 AS status 
               ,ord.start_loc              AS start_loc 
               ,ord.end_loc                AS end_loc 
               ,NVL(ct.country_code,'nal') AS country_code
        FROM 
        (
            SELECT  order_id                        AS order_id 
                   ,passenger_id                    AS user_id 
                   ,driver_id                       AS driver_id 
                   ,create_time                     AS create_time 
                   ,status                          AS status 
                   ,city_id                         AS city_id 
                   ,concat(start_lat,'_',start_lng) AS start_loc 
                   ,concat(end_lat,'_',end_lng)     AS end_loc
            FROM oride_dw.dwd_oride_order_base_include_test_di
            WHERE dt = '{pt}' 
            AND status IN (4,5,6)  
        ) ord
        LEFT JOIN 
        (
            SELECT  cit.id           AS city_id 
                   ,cty.country_code AS country_code
            FROM 
            (
                SELECT  id 
                       ,country
                FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df
                WHERE dt = '{pt}'  
            ) cit
            LEFT JOIN 
            (
                SELECT  country_name_en 
                       ,country_code
                FROM oride_dw.dim_oride_country_base 
            ) cty
            ON cit.country = cty.country_name_en 
        ) ct
        ON ord.city_id = ct.city_id ), 
        
        event_loc_data AS (
        SELECT  t.event_name                                                             AS event_name 
               ,t.order_id                                                               AS order_id 
               ,concat(substring(CAST(t.event_time AS string),0,10),'_',t.lat,'_',t.lng) AS loc
        FROM 
        (
            SELECT  s.event_name AS event_name 
                   ,s.event_time AS event_time 
                   ,s.order_id   AS order_id 
                   ,s.lat        AS lat 
                   ,s.lng        AS lng
            FROM 
            (
                SELECT  event_name 
                       ,event_time 
                       ,get_json_object(event_value,'$.order_id')                                                                AS order_id 
                       ,get_json_object(event_value,'$.lat')                                                                     AS lat 
                       ,get_json_object(event_value,'$.lng')                                                                     AS lng 
                       ,ROW_NUMBER() OVER(PARTITION BY event_name,get_json_object(event_value,'$.order_id') ORDER BY event_time) AS order_by
                FROM oride_dw.dwd_oride_client_event_detail_hi
                WHERE dt = '{pt}' 
                AND event_name IN ( 'accept_order_click', 'push_accept_order_click', 'pick_up_passengers_sliding_arrived', 'start_ride_sliding', 'start_ride_sliding_arrived', 'cancel_order', 'request_a_ride_click', 'successful_order_show', 'rider_arrive_show', 'start_ride_show' , 'complete_the_order_show', 'successful_order_click_cancel' ) 
                AND get_json_object(event_value,'$.order_id') IS NOT NULL  
            ) s
            WHERE s.order_by = 1  
        ) t ), 
        
        middle_data_1 AS (
        SELECT  od.* 
               ,nvl(IF(l.event_name = 'accept_order_click',l.loc,''),'')                 AS d_accept_order_click 
               ,nvl(IF(l.event_name = 'push_accept_order_click',l.loc,''),'')            AS d_push_accept_order_click 
               ,nvl(IF(l.event_name = 'pick_up_passengers_sliding_arrived',l.loc,''),'') AS d_pick_up_passengers_sliding_arrived 
               ,nvl(IF(l.event_name = 'start_ride_sliding',l.loc,''),'')                 AS d_start_ride_sliding 
               ,nvl(IF(l.event_name = 'start_ride_sliding_arrived',l.loc,''),'')         AS d_start_ride_sliding_arrived 
               ,nvl(IF(l.event_name = 'cancel_order',l.loc,''),'')                       AS d_cancel_order 
               ,nvl(IF(l.event_name = 'request_a_ride_click',l.loc,''),'')               AS p_request_a_ride_click 
               ,nvl(IF(l.event_name = 'successful_order_show',l.loc,''),'')              AS p_successful_order_show 
               ,nvl(IF(l.event_name = 'rider_arrive_show',l.loc,''),'')                  AS p_rider_arrive_show 
               ,nvl(IF(l.event_name = 'start_ride_show',l.loc,''),'')                    AS p_start_ride_show 
               ,nvl(IF(l.event_name = 'complete_the_order_show',l.loc,''),'')            AS p_complete_the_order_show 
               ,nvl(IF(l.event_name = 'successful_order_click_cancel',l.loc,''),'')      AS p_successful_order_click_cancel
        FROM order_data od
        LEFT JOIN 
        (
            SELECT  order_id 
                   ,event_name 
                   ,loc
            FROM event_loc_data 
        ) l
        ON od.order_id = l.order_id ), 
        
        middle_data_2 AS (
        SELECT  m.order_id                                                       AS order_id 
               ,m.user_id                                                        AS user_id 
               ,m.driver_id                                                      AS driver_id 
               ,m.create_time                                                    AS create_time 
               ,m.status                                                         AS status 
               ,m.start_loc                                                      AS start_loc 
               ,m.end_loc                                                        AS end_loc 
               ,m.country_code                                                   AS country_code 
               ,CONCAT_WS('',collect_list(d_accept_order_click))                 AS d_accept_order_click 
               ,CONCAT_WS('',collect_list(d_push_accept_order_click))            AS d_push_accept_order_click 
               ,CONCAT_WS('',collect_list(d_pick_up_passengers_sliding_arrived)) AS d_pick_up_passengers_sliding_arrived 
               ,CONCAT_WS('',collect_list(d_start_ride_sliding))                 AS d_start_ride_sliding 
               ,CONCAT_WS('',collect_list(d_start_ride_sliding_arrived))         AS d_start_ride_sliding_arrived 
               ,CONCAT_WS('',collect_list(d_cancel_order))                       AS d_cancel_order 
               ,CONCAT_WS('',collect_list(p_request_a_ride_click))               AS p_request_a_ride_click 
               ,CONCAT_WS('',collect_list(p_successful_order_show))              AS p_successful_order_show 
               ,CONCAT_WS('',collect_list(p_rider_arrive_show))                  AS p_rider_arrive_show 
               ,CONCAT_WS('',collect_list(p_start_ride_show))                    AS p_start_ride_show 
               ,CONCAT_WS('',collect_list(p_complete_the_order_show))            AS p_complete_the_order_show 
               ,CONCAT_WS('',collect_list(p_successful_order_click_cancel))      AS p_successful_order_click_cancel
        FROM middle_data_1 m
        GROUP BY  m.order_id 
                 ,m.user_id 
                 ,m.driver_id 
                 ,m.create_time 
                 ,m.status 
                 ,m.start_loc 
                 ,m.end_loc 
                 ,m.country_code ) 
                 ,
                 
        driver_location AS (
        SELECT  tt.order_id                             AS order_id 
               ,concat_ws(',',collect_list(tt.loc_str)) AS loc_list
        FROM 
        (
            SELECT  t.order_id 
                   ,t.loc_str
            FROM 
            (
                SELECT  order_id 
                       ,concat(`timestamp`,'_',lat,'_',lng) AS loc_str 
                       ,row_number() over(partition by order_id ORDER BY `timestamp`) order_by
                FROM oride_dw_ods.ods_log_driver_track_data_hi
                WHERE dt = '{pt}' 
                AND order_id <> 0  
            ) t
            WHERE t.order_by < 10000  
        ) tt
        JOIN order_data o
        ON tt.order_id = o.order_id
        GROUP BY  tt.order_id ) 
        
        
        -- user_location AS (
        -- SELECT  tt.order_id                             AS order_id 
        --        ,concat_ws(',',collect_list(tt.loc_str)) AS loc_list
        -- FROM 
        -- (
        -- 	SELECT  t.order_id 
        -- 	       ,t.loc_str
        -- 	FROM 
        -- 	(
        -- 		SELECT  order_id 
        -- 		       ,concat(`timestamp`,'_',lat,'_',lng) AS loc_str 
        -- 		       ,row_number() over(partition by order_id ORDER BY `timestamp`) order_by
        -- 		FROM oride_dw_ods.ods_log_user_track_data_hi
        -- 		WHERE dt = '{pt}' 
        -- 		AND order_id <> 0  
        -- 	) t
        -- 	WHERE t.order_by < 10000  
        -- ) tt
        -- JOIN order_data o
        -- ON tt.order_id = o.order_id
        -- GROUP BY  tt.order_id )
        
        INSERT OVERWRITE TABLE {db}.{table} PARTITION(country_code,dt)
        SELECT  
                rpad(reverse(m.order_id),16,'0') 
               ,m.order_id 
               ,m.user_id 
               ,m.driver_id 
               ,m.create_time 
               ,m.status 
               ,m.start_loc 
               ,m.end_loc 
               ,m.d_accept_order_click 
               ,m.d_push_accept_order_click 
               ,m.d_pick_up_passengers_sliding_arrived 
               ,m.d_start_ride_sliding 
               ,m.d_start_ride_sliding_arrived 
               ,m.d_cancel_order 
               ,m.p_request_a_ride_click 
               ,m.p_successful_order_show 
               ,m.p_rider_arrive_show 
               ,m.p_start_ride_show 
               ,m.p_complete_the_order_show 
               ,m.p_successful_order_click_cancel 
               ,nvl(d.loc_list,'') AS d_loc_list 
            --    ,nvl(u.loc_list,'') AS p_loc_list 
               ,'' 				   AS p_loc_list
               ,m.country_code     AS country_code 
               ,'{pt}'             AS dt
        FROM middle_data_2 m
        LEFT JOIN driver_location d ON m.order_id = d.order_id 
        -- LEFT JOIN user_location u
        
        ;

'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''

      SELECT count(1)-count(distinct order_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('nal')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_order_anti_cheating_location_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_order_anti_cheating_location_di_task = PythonOperator(
    task_id='dwd_oride_order_anti_cheating_location_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

location_to_hbase = HiveOperator(
    task_id='location_to_hbase',
    hql="""
        INSERT OVERWRITE TABLE oride_hbase.dwd_oride_order_anti_cheating_location_di
        SELECT
            rowkey,
            order_id,
            user_id,
            driver_id,
            create_time,
            status,
            start_loc,
            end_loc,
            d_accept_order_click,
            d_push_accept_order_click,
            d_pick_up_passengers_sliding_arrived,
            d_start_ride_sliding,
            d_start_ride_sliding_arrived,
            d_cancel_order,
            p_request_a_ride_click,
            p_successful_order_show,
            p_rider_arrive_show,
            p_start_ride_show,
            p_complete_the_order_show,
            p_successful_order_click_cancel,
            d_loc_list,
            p_loc_list,
            country_code,
            dt
        FROM
            oride_dw.dwd_oride_order_anti_cheating_location_di
        WHERE
            dt = '{{ ds }}'
    """,
    schema='oride_hbase',
    dag=dag)



dependence_dwd_oride_client_event_detail_hi_prev_day_task >> \
dependence_ods_log_driver_track_data_hi_prev_day_task >> \
dependence_ods_log_user_track_data_hi_prev_day_task >> \
dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> dwd_oride_order_anti_cheating_location_di_task >> location_to_hbase
