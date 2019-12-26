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

dwd_oride_order_base_include_test_di_prev_day_task = S3KeySensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-bi',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_location_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": db_name, "table": "{dag_name}".format(dag_name=dag_ids),
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


def dwd_oride_order_location_di_sql_task(ds):
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
        AND status = 5  
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
            AND event_name IN ( 'looking_for_a_driver_show', 'successful_order_show', 'accept_order_click', 'rider_arrive_show', 'confirm_arrive_click_arrived', 'pick_up_passengers_sliding_arrived', 'start_ride_show', 'start_ride_sliding', 'complete_the_order_show', 'start_ride_sliding_arrived' ) 
            AND get_json_object(event_value,'$.order_id') IS NOT NULL  
        ) s
        WHERE s.order_by = 1  
        ) t ), 
        
        middle_data_1 AS (
        SELECT  od.* 
               ,nvl(IF(l.event_name = 'looking_for_a_driver_show',l.loc,''),'')          AS looking_for_a_driver_show 
               ,nvl(IF(l.event_name = 'successful_order_show',l.loc,''),'')              AS successful_order_show 
               ,nvl(IF(l.event_name = 'accept_order_click',l.loc,''),'')                 AS accept_order_click 
               ,nvl(IF(l.event_name = 'rider_arrive_show',l.loc,''),'')                  AS rider_arrive_show 
               ,nvl(IF(l.event_name = 'confirm_arrive_click_arrived',l.loc,''),'')       AS confirm_arrive_click_arrived 
               ,nvl(IF(l.event_name = 'pick_up_passengers_sliding_arrived',l.loc,''),'') AS pick_up_passengers_sliding_arrived 
               ,nvl(IF(l.event_name = 'start_ride_show',l.loc,''),'')                    AS start_ride_show 
               ,nvl(IF(l.event_name = 'start_ride_sliding',l.loc,''),'')                 AS start_ride_sliding 
               ,nvl(IF(l.event_name = 'complete_the_order_show',l.loc,''),'')            AS complete_the_order_show 
               ,nvl(IF(l.event_name = 'start_ride_sliding_arrived',l.loc,''),'')         AS start_ride_sliding_arrived
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
        SELECT  m.order_id                                                     AS order_id 
               ,m.user_id                                                      AS user_id 
               ,m.driver_id                                                    AS driver_id 
               ,m.create_time                                                  AS create_time 
               ,m.status                                                       AS status 
               ,m.start_loc                                                    AS start_loc 
               ,m.end_loc                                                      AS end_loc 
               ,m.country_code                                                 AS country_code 
               ,CONCAT_WS('',collect_list(looking_for_a_driver_show))          AS looking_for_a_driver_show 
               ,CONCAT_WS('',collect_list(successful_order_show))              AS successful_order_show 
               ,CONCAT_WS('',collect_list(accept_order_click))                 AS accept_order_click 
               ,CONCAT_WS('',collect_list(rider_arrive_show))                  AS rider_arrive_show 
               ,CONCAT_WS('',collect_list(confirm_arrive_click_arrived))       AS confirm_arrive_click_arrived 
               ,CONCAT_WS('',collect_list(pick_up_passengers_sliding_arrived)) AS pick_up_passengers_sliding_arrived 
               ,CONCAT_WS('',collect_list(start_ride_show))                    AS start_ride_show 
               ,CONCAT_WS('',collect_list(start_ride_sliding))                 AS start_ride_sliding 
               ,CONCAT_WS('',collect_list(complete_the_order_show))            AS complete_the_order_show 
               ,CONCAT_WS('',collect_list(start_ride_sliding_arrived))         AS start_ride_sliding_arrived
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
        
        INSERT OVERWRITE TABLE {db}.{table} PARTITION(country_code,dt)
        SELECT  m.order_id                           AS order_id --订单id 
               ,m.user_id                            AS user_id --乘客id 
               ,m.driver_id                          AS driver_id --司机id 
               ,m.create_time                        AS create_time --订单创建时间 
               ,m.status                             AS status --订单状态 
               ,m.start_loc                          AS start_loc --起始经纬度 
               ,m.end_loc                            AS end_loc --终点经纬度 
               ,m.looking_for_a_driver_show          AS looking_for_a_driver_show -- looking_for_a_driver_show event经纬度 
               ,m.successful_order_show              AS successful_order_show -- successful_order_show event经纬度 
               ,m.accept_order_click                 AS accept_order_click -- accept_order_click event经纬度 
               ,m.rider_arrive_show                  AS rider_arrive_show -- rider_arrive_show event经纬度 
               ,m.confirm_arrive_click_arrived       AS confirm_arrive_click_arrived -- confirm_arrive_click_arrived event经纬度 
               ,m.pick_up_passengers_sliding_arrived AS pick_up_passengers_sliding_arrived -- pick_up_passengers_sliding_arrived event经纬度 
               ,m.start_ride_show                    AS start_ride_show -- start_ride_show event经纬度 
               ,m.start_ride_sliding                 AS start_ride_sliding -- start_ride_sliding event经纬度 
               ,m.complete_the_order_show            AS complete_the_order_show -- complete_the_order_show event经纬度 
               ,m.start_ride_sliding_arrived         AS start_ride_sliding_arrived -- start_ride_sliding_arrived event经纬度 
               ,m.loc_list                           AS loc_list -- 司机轨迹数据 
               ,m.country_code                       AS country_code -- 国家码 
               ,m.dt                                 AS dt -- 时间分区
        FROM 
        (
        SELECT  row_number() over(partition by m.order_id ORDER BY m.create_time DESC) id 
               ,m.order_id 
               ,m.user_id 
               ,m.driver_id 
               ,m.create_time 
               ,m.status 
               ,m.start_loc 
               ,m.end_loc 
               ,m.looking_for_a_driver_show 
               ,m.successful_order_show 
               ,m.accept_order_click 
               ,m.rider_arrive_show 
               ,m.confirm_arrive_click_arrived 
               ,m.pick_up_passengers_sliding_arrived 
               ,m.start_ride_show 
               ,m.start_ride_sliding 
               ,m.complete_the_order_show 
               ,m.start_ride_sliding_arrived 
               ,m.loc_list 
               ,m.country_code 
               ,m.dt
        FROM 
        (
            SELECT  m.order_id 
                   ,m.user_id 
                   ,m.driver_id 
                   ,m.create_time 
                   ,m.status 
                   ,m.start_loc 
                   ,m.end_loc 
                   ,m.looking_for_a_driver_show 
                   ,m.successful_order_show 
                   ,m.accept_order_click 
                   ,m.rider_arrive_show 
                   ,m.confirm_arrive_click_arrived 
                   ,m.pick_up_passengers_sliding_arrived 
                   ,m.start_ride_show 
                   ,m.start_ride_sliding 
                   ,m.complete_the_order_show 
                   ,m.start_ride_sliding_arrived 
                   ,nvl(d.loc_list,'') AS loc_list 
                   ,m.country_code     AS country_code 
                   ,'{pt}'             AS dt
            FROM middle_data_2 m
            LEFT JOIN driver_location d
            ON m.order_id = d.order_id 
        ) m 
        ) m
        WHERE m.id = 1 ;  

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
    _sql = dwd_oride_order_location_di_sql_task(ds)

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


dwd_oride_order_location_di_task = PythonOperator(
    task_id='dwd_oride_order_location_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_client_event_detail_hi_prev_day_task >> \
dependence_dwd_oride_driver_location_event_hi_prev_day_task >> \
dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> dwd_oride_order_location_di_task
