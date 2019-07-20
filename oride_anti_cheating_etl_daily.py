from datetime import datetime, timedelta

import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 7, 15),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_anti_cheating_etl_daily',
    schedule_interval="40 02 * * *",
    default_args=args)


insert_order_location_info = HiveOperator(
    task_id='insert_order_location_info',
    hql='''
        set hive.execution.engine=mr;
        set mapreduce.map.java.opts=-Xmx3072m -XX:-UseGCOverheadLimit;
        set mapreduce.reduce.java.opts=-Xmx2048m;
        set mapreduce.map.memory.mb=3072;
        set mapreduce.reduce.memory.mb=3072;
        with order_data as (
            select 
            id,
            user_id,
            driver_id,
            create_time,
            status,
            concat(start_lat,'_',start_lng) start_loc,
            concat(end_lat,'_',end_lng) end_loc
            from oride_db.data_order 
            where dt = '{{ ds }}'
            and from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}'
            and (status = 4 or status = 5)
        ),
        
        
        event_loc_data as (
            select 
            t.event_name event_name,
            t.order_id order_id,
            concat(t.lat,'_',t.lng) loc
            from 
            (
                select
                event_name,
                get_json_object(event_value,'$.order_id') order_id,
                get_json_object(event_value,'$.lat') lat,
                get_json_object(event_value,'$.lng') lng
                from oride_bi.oride_client_event_detail
                where dt = '{{ ds }}'
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
            )  l on od.id = l.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'successful_order_show'
            )  s on od.id = s.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'accept_order_click'
            )  a on od.id = a.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'rider_arrive_show'
            )  r on od.id = r.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'confirm_arrive_click_arrived'
            ) c on od.id = c.order_id
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
            ) p on m.id = p.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_show'
            ) s on m.id = s.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_sliding'
            ) st on m.id = st.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'complete_the_order_show'
            ) c on m.id = c.order_id
        
            left join (
                select 
                order_id,
                loc 
                from event_loc_data 
                where event_name = 'start_ride_sliding_arrived'
            ) sta on m.id = sta.order_id
        ),
        
        
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}'
            group by order_id
        )
        
        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
        select 
        m.*,
        nvl(d.loc_list,'') loc_list
        from middle_data_2 m 
        left join driver_location d on m.id = d.order_id
        ;


        ''',
    schema='oride_bi',
    dag=dag)


clear_order_location_mysql_data = MySqlOperator(
    task_id='clear_order_location_mysql_data',
    sql="""
        DELETE FROM oride_order_location_info WHERE dt='{{ ds }}';
    """,
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
            loc_list string
            from oride_bi.oride_order_location_info
            where dt='{{ ds }}'
            
        """,
    mysql_conn_id='mysql_bi',
    mysql_table='oride_order_location_info',
    dag=dag)

insert_order_location_info >> clear_order_location_mysql_data >> order_location_info_to_msyql
