from datetime import datetime, timedelta

import airflow
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 6, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_log_etl_daily',
    schedule_interval="10 00 * * *",
    default_args=args)

dispatch_table = HiveOperator(
    task_id='dispatch_table',
    hql='''
        insert overwrite table oride_bi.server_magic_dispatch_detail
        partition(dt='{{ ds }}')
        select 
        get_json_object(event_values, '$.order_id') order_id, 
        get_json_object(event_values, '$.round') as `round`, 
        get_json_object(event_values, '$.user_id') as `user_id`,
        driver_id,
        get_json_object(event_values, '$.city_id') city_id
        from  
        oride_source.dispatch_tracker_server_magic 
        lateral view explode(split(substr(get_json_object(event_values, '$.driver_ids'),1,length(get_json_object(event_values, '$.driver_ids'))-2),',')) driver_ids as driver_id
        where  dt = '{{ ds }}' and event_name='dispatch_chose_driver' 
        ''',
    schema='oride_source',
    dag=dag)

filter_table = HiveOperator(
    task_id='filter_table',
    hql='''
        insert overwrite table oride_bi.server_magic_filter_detail
        partition(dt='{{ ds }}')
        select 
        get_json_object(event_values, '$.order_id') order_id, 
        get_json_object(event_values, '$.round') as `round`, 
        get_json_object(event_values, '$.user_id') as `user_id`,
        get_json_object(event_values, '$.driver_id') as `driver_id`,
        get_json_object(event_values, '$.reason') as `reason`,
        get_json_object(event_values, '$.city_id') city_id
        from  
        oride_source.dispatch_tracker_server_magic 
        where  dt = '{{ ds }}' and event_name='dispatch_filter_driver' 
        ''',
    schema='oride_source',
    dag=dag)

assign_table = HiveOperator(
    task_id='assign_table',
    hql='''
        insert overwrite table oride_bi.server_magic_assign_detail
        partition(dt='{{ ds }}')
        select 
        get_json_object(event_values, '$.order_id') order_id, 
        get_json_object(event_values, '$.round') as `round`, 
        get_json_object(event_values, '$.user_id') as `user_id`,
        driver_id,
        get_json_object(event_values, '$.city_id') city_id
        from  
        oride_source.dispatch_tracker_server_magic 
        lateral view explode(split(substr(get_json_object(event_values, '$.driver_ids'),1,length(get_json_object(event_values, '$.driver_ids'))-2),',')) driver_ids as driver_id
        where  dt = '{{ ds }}' and event_name='dispatch_assign_driver' 
        ''',
    schema='oride_source',
    dag=dag)

push_table = HiveOperator(
    task_id='push_table',
    hql='''
        insert overwrite table oride_bi.server_magic_push_detail
        partition(dt='{{ ds }}')
        select 
        get_json_object(event_values, '$.order_id') order_id, 
        get_json_object(event_values, '$.round') as `round`, 
        get_json_object(event_values, '$.user_id') as `user_id`,
        get_json_object(event_values, '$.driver_id') as `driver_id`,
        get_json_object(event_values, '$.success') as `success`,
        get_json_object(event_values, '$.city_id') city_id
        from  
        oride_source.dispatch_tracker_server_magic 
        where  dt = '{{ ds }}' and event_name='dispatch_push_driver' 
        ''',
    schema='oride_source',
    dag=dag)


dispatch_table >> filter_table >> assign_table >> push_table