from datetime import datetime, timedelta

import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

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


validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names":
            [
             'oride_dw.ods_log_driver_track_data_hi ',
             'oride_bi.oride_client_event_detail',
             'oride_db.data_order',
             ],
        # 任务名称
        "task_name": "订单反作弊轨迹数据"
    },
    dag=dag
)



data_order_validate_task = HivePartitionSensor(
    task_id="data_order_validate_task",
    table="data_order",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



client_event_data_order_validate_task = HivePartitionSensor(
    task_id="data_order_validate_task",
    table="oride_client_event_detail",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)




driver_track_validate_task = HivePartitionSensor(
    task_id="driver_track_validate_task",
    table="ods_log_driver_track_data_hi",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



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
            concat(substring(cast(t.event_time as string),0,10),'_',t.lat,'_',t.lng) loc
            from 
            (
                select
                event_name,
                event_time ,
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
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
        select 
        m.*,
        ''
        from middle_data_2 m 
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_0_0 = HiveOperator(
    task_id='insert_order_loc_list_0_0',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '00' and '00'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_1_1 = HiveOperator(
    task_id='insert_order_loc_list_1_1',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '01' and '01'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_2_2 = HiveOperator(
    task_id='insert_order_loc_list_2_2',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '02' and '02'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_3_3 = HiveOperator(
    task_id='insert_order_loc_list_3_3',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '03' and '03'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_4_4 = HiveOperator(
    task_id='insert_order_loc_list_4_4',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '04' and '04'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_5_5 = HiveOperator(
    task_id='insert_order_loc_list_5_5',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '05' and '05'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_6_6 = HiveOperator(
    task_id='insert_order_loc_list_6_6',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '06' and '06'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_7_7 = HiveOperator(
    task_id='insert_order_loc_list_7_7',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '07' and '07'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_8_8 = HiveOperator(
    task_id='insert_order_loc_list_8_8',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '08' and '08'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_9_9 = HiveOperator(
    task_id='insert_order_loc_list_9_9',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '09' and '09'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_10_10 = HiveOperator(
    task_id='insert_order_loc_list_10_10',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '10' and '10'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_11_11 = HiveOperator(
    task_id='insert_order_loc_list_11_11',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '11' and '11'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_12_12 = HiveOperator(
    task_id='insert_order_loc_list_12_12',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '12' and '12'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_13_13 = HiveOperator(
    task_id='insert_order_loc_list_13_13',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '13' and '13'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_14_14 = HiveOperator(
    task_id='insert_order_loc_list_14_14',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '14' and '14'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_15_15 = HiveOperator(
    task_id='insert_order_loc_list_15_15',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '15' and '15'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)

insert_order_loc_list_16_16 = HiveOperator(
    task_id='insert_order_loc_list_16_16',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '16' and '16'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)




insert_order_loc_list_17_17 = HiveOperator(
    task_id='insert_order_loc_list_17_17',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '17' and '17'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)



insert_order_loc_list_18_18 = HiveOperator(
    task_id='insert_order_loc_list_18_18',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '18' and '18'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)




insert_order_loc_list_19_19 = HiveOperator(
    task_id='insert_order_loc_list_19_19',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '19' and '19'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)



insert_order_loc_list_20_20 = HiveOperator(
    task_id='insert_order_loc_list_20_20',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '20' and '20'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)



insert_order_loc_list_21_21 = HiveOperator(
    task_id='insert_order_loc_list_21_21',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '21' and '21'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)





insert_order_loc_list_22_22 = HiveOperator(
    task_id='insert_order_loc_list_22_22',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '22' and '22'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)



insert_order_loc_list_23_23 = HiveOperator(
    task_id='insert_order_loc_list_23_23',
    hql='''

        with 
        driver_location as (
            select 
            order_id,
            concat_ws(',',collect_list(concat(`timestamp`,'_',lat,'_',lng))) loc_list
            from oride_dw.ods_log_driver_track_data_hi 
            where dt = '{{ ds }}' and hour between '23' and '23'
            group by order_id
        )

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        if(d.order_id is null,m.loc_list,d.loc_list)
        from 
        (   
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
            m.loc_list
            from
            oride_bi.oride_order_location_info m
            where m.dt = '{{ ds }}'
        ) m 
        left join driver_location d on m.order_id = d.order_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)


order_distinct = HiveOperator(
    task_id='order_distinct',
    hql='''

        insert overwrite table oride_bi.oride_order_location_info partition(dt='{{ ds }}')
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
        m.loc_list
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
        m.loc_list
        from oride_bi.oride_order_location_info m 
        where 
        m.dt = '{{ ds }}'
        ) m
        where id = 1
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



validate_partition_data >> data_order_validate_task >> insert_order_location_info
validate_partition_data >> client_event_data_order_validate_task >> insert_order_location_info
validate_partition_data >> driver_track_validate_task >> insert_order_location_info
insert_order_location_info >> \
insert_order_loc_list_0_0 >> \
insert_order_loc_list_1_1 >> \
insert_order_loc_list_2_2 >> \
insert_order_loc_list_3_3 >> \
insert_order_loc_list_4_4 >> \
insert_order_loc_list_5_5 >> \
insert_order_loc_list_6_6 >> \
insert_order_loc_list_7_7 >> \
insert_order_loc_list_8_8 >> \
insert_order_loc_list_9_9 >> \
insert_order_loc_list_10_10 >> \
insert_order_loc_list_11_11 >> \
insert_order_loc_list_12_12 >> \
insert_order_loc_list_13_13 >> \
insert_order_loc_list_14_14 >> \
insert_order_loc_list_15_15 >> \
insert_order_loc_list_16_16 >> \
insert_order_loc_list_17_17 >> \
insert_order_loc_list_18_18 >> \
insert_order_loc_list_19_19 >> \
insert_order_loc_list_20_20 >> \
insert_order_loc_list_21_21 >> \
insert_order_loc_list_22_22 >> \
insert_order_loc_list_23_23 >> \
order_distinct >> \
clear_order_location_mysql_data >> order_location_info_to_msyql
