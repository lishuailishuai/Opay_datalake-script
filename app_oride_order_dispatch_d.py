# -*- coding: utf-8 -*-
"""
调度算法效果监控指标新版2019-08-02
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
#from airflow.sensors.hdfs_sensor import HdfsSensor
from airflow.sensors import WebHdfsSensor
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
from datetime import datetime, timedelta
import re
import logging

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_order_dispatch_d',
    schedule_interval="00 01 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)

"""
##----依赖数据源---##
"""
dependence_ods_oride_server_magic = UFileSensor(
    task_id='dependence_ods_oride_server_magic',
    filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
        hdfs_path_str="oride/server_magic",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,                                           #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_ods_oride_client_event = WebHdfsSensor(
    task_id='dependence_ods_oride_client_event',
    filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
        hdfs_path_str="/user/hive/warehouse/oride_bi.db/oride_client_event_detail",
        pt='{{ ds }}'
    ),
    dag=dag
)

dependence_ods_oride_data_order = UFileSensor(
    task_id='dependence_ods_oride_data_order',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_order",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_driver_records_day = UFileSensor(
    task_id='dependence_ods_oride_data_driver_records_day',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_driver_records_day",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_driver_recharge_records = UFileSensor(
    task_id='dependence_ods_oride_data_driver_recharge_records',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_driver_recharge_records",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_driver_reward = UFileSensor(
    task_id='dependence_ods_oride_data_driver_reward',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_driver_reward",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_driver_timerange = WebHdfsSensor(
    task_id='dependence_ods_oride_driver_timerange',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="/user/hive/warehouse/oride_bi.db/oride_driver_timerange",
        pt='{{ ds }}'
    ),
    dag=dag
)
"""
##-----end-----##
"""

hive_table = 'oride_dw.app_oride_order_dispatch_d'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {hive_table} (
            city_id bigint comment '城市ID',
            city_name string comment '城市名称',
            product_id bigint comment '业务线',
            broadcast_distance decimal(10,2) comment '平均播单距离',
            pickup_distance_done decimal(10,2) comment '平均接驾距离(完单)',
            pickup_distance_take decimal(10,2) comment '平均接驾距离(应答)',
            dispatch_obey_rate decimal(10,2) comment '调度服从率',
            take_rate decimal(10,2) comment '应答率',
            take_order_dur_done decimal(10,2) comment '平均应答时长(完单)',
            take_order_dur_take decimal(10,2) comment '平均应答时长(应答)',
            passanger_after_cancel_rate decimal(10,2) comment '乘客应答后取消率',
            driver_after_cancel_rate decimal(10,2) comment '司机应答后取消率',
            tph_macro decimal(10,2) comment 'tph', 
            online_range decimal(10,2) comment '平均在线时长',
            billing_order_dur_rate decimal(10,2) comment '计费时长占比', 
            ride_order_cnt bigint comment '下单数',
            broadcast_cnt bigint comment '播单数',
            succ_broadcast_cnt bigint comment '成功播单数',
            broadcast_rate decimal(10,2) comment '播单率',
            request_order_cnt bigint comment '接单数',
            request_rate decimal(10,2) comment '接单率',
            dispatch_request_rate decimal(10,2) comment '调度接单率',
            finish_order_cnt bigint comment '完单数',
            finish_rate decimal(10,2) comment '完单率',
            dispatch_finish_rate decimal(10,2) comment '调度完单率',
            tph_order decimal(10,2) comment 'tph',
            send_done_distance decimal(10,2) comment '平均送驾距离', 
            pick_up_order_dur decimal(10,2) comment '平均接驾时长',
            billing_order_dur decimal(10,2) comment '平均计费时长',
            pay_order_dur decimal(10,2) comment '平均支付时长',
            drivers_online bigint comment '在线司机数',
            drivers_take bigint comment '接单司机数',
            drivers_finished bigint comment '完单司机数',
            finished_per_driver bigint comment '人均完单数',
            push_per_driver bigint comment '人均推送订单数',
            take_per_driver bigint comment '人均应答订单数',
            driver_obey_rate decimal(10,2) comment '司机服从率',
            driver_online_dur decimal(10,2) comment '人均在线时长',
            driver_service_dur decimal(10,2) comment '人均服务时长',
            driver_iph_done decimal(10,2) comment '完单司机IPH', 
            driver_salary_done decimal(10,2) comment '完单司机日薪', 
            push_arrive_rate decimal(10,2) comment '播报到达率',
            driver_push_cnt bigint comment '人均推单次数',
            driver_pushed_cnt bigint comment '平均推送司机数',
            cancel_order_cnt bigint comment '取消订单数',
            sys_cancel_order_cnt bigint comment '系统取消订单数',
            sys_cancel_rate decimal(10,2) comment '系统取消率',
            passanger_before_cancel_order_cnt bigint comment '乘客应答前取消数',
            passanger_before_cancel_order_rate decimal(10,2) comment '乘客应答前取消率',
            after_cancel_order_cnt bigint comment '应答后取消数',
            passanger_after_cancel_order_cnt bigint comment '乘客应答后取消数',
            passanger_after_cancel_order_dur decimal(10,2) comment '乘客应答后取消平均时长（分）',
            passanger_after_cancel_order_rate decimal(10,2) comment '乘客应答后取消率',
            passanger_cancel_dis decimal(10,2) comment '乘客取消订单平均接驾距离',
            driver_after_cancel_order_cnt bigint comment '司机应答后取消数',
            driver_after_cancel_order_dur decimal(10,2) comment '司机应答后取消平均时长（分）',
            driver_after_cancel_order_rate decimal(10,2) comment '司机应答后取消率',
            driver_take_num bigint comment '司机应答总次数',
            take_num_per_driver bigint comment '司机人均应答次数',
            valid_orders bigint comment '有效下单数',
            valid_orders_finished decimal(10,2) comment '有效完单率'
        )
        PARTITIONED BY (
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
            )
        STORED AS PARQUET
    '''.format(hive_table=hive_table),
    schema='oride_dw',
    dag=dag
)


def drop_partions(*op_args, **op_kwargs):
    dt = op_kwargs['ds']
    cursor = get_hive_cursor()
    sql = '''
        show partitions {hive_table}
    '''.format(hive_table=hive_table)
    cursor.execute(sql)
    res = cursor.fetchall()
    logging.info(res)
    for partition in res:
        prt, = partition
        matched = re.search(r'country_code=(?P<cc>\w+)/dt=(?P<dy>.*)$', prt)
        cc = matched.groupdict().get('cc', 'nal')
        dy = matched.groupdict().get('dy', '')
        if dy == dt:
            hql = '''
                ALTER TABLE {hive_table} DROP IF EXISTS PARTITION (country_code='{cc}', dt = '{dt}')
            '''.format(cc=cc, dt=dt, hive_table=hive_table)
            logging.info(hql)
            cursor.execute(hql)


drop_partitons_from_table = PythonOperator(
    task_id='drop_partitons_from_table',
    python_callable=drop_partions,
    provide_context=True,
    dag=dag
)


insert_result_to_hive = HiveOperator(
    task_id='insert_result_to_hive',
    hql='''
        set hive.execution.engine=tez;
        set hive.prewarm.enabled=true;
        set hive.prewarm.numcontainers=16;
        --set hive.exec.parallel=true;
        WITH 
        push_distance_finished as (
            select 
                get_json_object(sm.event_values, '$.city_id') as city_id,
                nvl(do.serv_type, 0) as serv_type,
	            sm.dt,
	            round(sum(get_json_object(event_values, '$.distance'))/count(1), 2) as avg_distance             --平均播单距离 
            from (select * from oride_source.server_magic where dt='{pt}') as sm 
            inner join (select * from oride_db.data_order where dt='{pt}') as do  
            where cast(get_json_object(sm.event_values, '$.order_id') as bigint) = do.id and 
                sm.dt = '{pt}' and 
                do.dt = '{pt}' and 
	            sm.event_name = 'dispatch_push_driver' and 
	            get_json_object(sm.event_values, '$.success') = 1 
            group by sm.dt, get_json_object(sm.event_values, '$.city_id'), nvl(do.serv_type, 0)
        ), 
        pick_distance_finished as (
            select 
                o.city_id,
                o.serv_type,
				m.dt,
				round(sum(if(o.status in (4, 5), m.dis, 0))/sum(if(o.status in (4, 5), 1, 0)), 2) as avg_distance,      --平均接驾距离(完单)
				round(sum(if(o.status=6 and o.cancel_role=1, m.dis, 0))/sum(if(o.status=6 and o.cancel_role=1, 1, 0)), 2) as avg_distance_us_cancel --平均用户取消订单接驾距离
			from 
			    (select 
	    			*
    			from 
					(select 
						dt,
						split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''),']',''), ',') as drivers, 
						split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
						get_json_object(event_values, '$.order_id') as order_id
					from oride_source.server_magic 
					where dt='{pt}' and 
						event_name='dispatch_assign_driver' and 
		    			from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
	    			) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id 
	    			lateral view posexplode(distances) ds as dspos, dis 
	    			where dpos = dspos
    			) as m inner join (select * from oride_db.data_order where dt='{pt}') as o 
			where cast(m.order_id as int) = o.id and 
				o.dt = '{pt}' and 
				from_unixtime(o.create_time, 'yyyy-MM-dd') = '{pt}'
			group by m.dt, o.city_id, o.serv_type
        ),
        pick_distance_taked as (
            select 
                do.city_id,
                do.serv_type,
    			m.dt,
    			round(sum(m.dis)/count(1), 2) as avg_distance,      --平均接驾距离(应答)
			    count(distinct do.id) as orders_clicked             --应答订单数
		    from 
    			(select 
        			*
    			from 
        			(select 
            			dt,
        				split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''), ']',''), ',') as drivers, 
        				split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
        				get_json_object(event_values, '$.order_id') as order_id
    				from oride_source.server_magic 
				    where dt='{pt}' and 
					    event_name='dispatch_assign_driver' 
				    ) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id 
				    lateral view posexplode(distances) ds as dspos, dis 
				    where dpos = dspos
	    		) as m inner join (
	    		    select 
                        distinct get_json_object(event_value, '$.order_id') as id 
                    from oride_bi.oride_client_event_detail 
                    where dt='{pt}' and 
                        event_name = 'accept_order_click'
            	) as o inner join (select * from oride_db.data_order where dt='{pt}') do 
		    where cast(m.order_id as bigint) = cast(o.id as bigint) and 
		        cast(m.order_id as bigint) = do.id and 
		        do.dt = '{pt}'
		    group by m.dt, do.city_id, do.serv_type
        ),
        click_data as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            count(1) as click_count,                                        --司机应答总次数
	            count(distinct ce.user_id) as drivers_clicked,                  --应答司机数
	            count(1)/count(distinct ce.user_id) as avg_clicked              --人均应答次数
            from
                (select 
                    cast(get_json_object(event_value, '$.order_id') as bigint) as order_id,
                    user_id
                from oride_bi.oride_client_event_detail 
                where dt = '{pt}' and 
                    event_name = 'accept_order_click'
                ) as ce 
            inner join (select * from oride_db.data_order where dt='{pt}') do  
            where ce.order_id = do.id and 
                do.dt = '{pt}' and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}' 
            group by do.dt, do.city_id, do.serv_type
        ),
        push_data as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            count(distinct if(success=1, t.order_id, null)) as orders_pushed,      --成功推送订单数
	            count(distinct if(success=1, t.driver_id, null)) as drivers_pushed,    --成功推送司机数
	            count(distinct if(success=1, t.order_id, null))/count(distinct if(success=1, t.driver_id, null)) as avg_order_pushed, --人均推送订单数
	            sum(if(success=1, 1, 0)) as count_pushed,    --推送给司机的总次数
	            count(distinct t.order_id) as order_push       --推单数
            from (
                select 
                    cast(get_json_object(event_values, '$.success') as int) as success,
                    cast(get_json_object(event_values, '$.order_id') as bigint) as order_id,
                    cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id
                from oride_source.server_magic 
                where dt='{pt}' and 
	                event_name='dispatch_push_driver' and  
	                from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
	            ) as t 
            inner join (select * from oride_db.data_order where dt='{pt}') do  
            where t.order_id = do.id and 
                do.dt = '{pt}' and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}' 
            group by do.dt, do.city_id, do.serv_type
        ),
        dirvers_pushed as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            sum(drivers) as drivers_per_round   --SUM(每个订单每轮被推送的司机数去重)
            from 
                (select 
		            count(distinct get_json_object(event_values, '$.driver_id')) as drivers,
		            cast(get_json_object(event_values, '$.order_id') as bigint) as order_id
                from oride_source.server_magic 
                where dt = '{pt}' and 
		            event_name = 'dispatch_push_driver' and 
		            from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd') = '{pt}' and 
		            get_json_object(event_values, '$.success') = 1
                group by get_json_object(event_values, '$.order_id'), get_json_object(event_values, '$.round')
                ) as t 
            inner join (select * from oride_db.data_order where dt='{pt}') as do  
            where t.order_id = do.id and 
                do.dt = '{pt}' and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') ='{pt}' 
            group by do.dt, do.city_id, do.serv_type
        ),
        orders_show as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            count(1) as click_count,                        --订单推送展示次数(推送成功次数)
                count(distinct t.user_id) as drivers_show,      --展示司机数（成功推送司机数）
                count(1)/count(distinct t.user_id) as avg_show  --人均展示次数
            from (
                select 
                    cast(get_json_object(event_value, '$.order_id') as bigint) as order_id,
                    user_id
                from oride_bi.oride_client_event_detail 
                where dt='{pt}' and 
                    event_name = 'accept_order_show'
                ) as t 
            inner join (select * from oride_db.data_order where dt='{pt}') as do  
            where t.order_id = do.id and 
                do.dt = '{pt}' and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}' 
            group by do.dt, do.city_id, do.serv_type
        ),
        orders_data as (
            select 
                city_id,
                serv_type,
                dt,
    			count(1) as orders, --下单数
    			sum(if(driver_id>0, 1, 0)) as orders_take, --有司机接单数
			    sum(if(status in (4,5), abs(take_time-create_time), 0)) as take_range, --总应答时长（完单秒）
    			sum(if(status in (4,5), 1, 0)) as orders_f, --完成订单数
    			sum(if(status in (4,5), abs(take_time-create_time), 0))/sum(if(status in (4,5), 1, 0)) as avg_take_range, --平均应答时长(完单秒)
			    sum(if(status=6 and driver_id>0 and cancel_role=1, 1, 0)) as cancel_af_take_us, --乘客应答后取消订单数
			    sum(if(status=6 and driver_id>0 and cancel_role=2, 1, 0)) as cancel_af_take_dr, --司机应答后取消订单数
			    sum(if(status=6, 1, 0)) as cancel_all,  --总取消订单数
			    sum(if(status=6 and cancel_role in (3,4), 1, 0)) as cancel_sys, --系统总取消订单数
			    sum(if(status=6 and driver_id=0 and cancel_role=1, 1, 0)) as cancel_bf_take_us, --乘客应答前取消订单数
			    sum(if(status=6 and driver_id>0, 1, 0)) as cancel_af_take_all, --应答后取消订单总数
			    sum(if(status=6 and driver_id>0 and cancel_role=1, abs(cancel_time-take_time), 0)) as cancel_af_take_us_range, --乘客应答后取消总时长(秒)
			    sum(if(status=6 and driver_id>0 and cancel_role=2, abs(cancel_time-take_time), 0)) as cancel_af_take_dr_range, --司机应答后取消总时长(秒)
			    sum(if(status in (4,5), abs(arrive_time-pickup_time), 0)) as billing_range, --总计费时长/总服务时长(秒)
			    sum(if(status in (4,5), abs(pickup_time - take_time), 0)) as pick_range, --总接驾时长(秒)
			    sum(if(status in (4,5), distance, 0)) as send_dis,          --总送驾距离(米)
			    sum(if(status=5, abs(finish_time-arrive_time), 0)) as pay_range, --总支付时长(秒)
			    sum(if(status=5, 1, 0)) as payed_orders --支付订单数
		    from oride_db.data_order 
		    where dt='{pt}' and 
    			from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
    		group by dt, city_id, serv_type 
        ),
        valid_orders as (
            SELECT 
                city_id,
                serv_type,
                dt,
        	    sum(if(id=id2, 1, 
        	        if(abs(create_time2-create_time)<=1800 and 
        	            2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 and 
        	            2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
        	        )
        	    )) as valid_order
    	    FROM (
    	        SELECT 
                    do.city_id,
                    do.serv_type,
                    do.dt,
            	    do.id, 
            	    do.user_id, 
            	    do.start_lng, 
            	    do.start_lat, 
            	    do.end_lng, 
            	    do.end_lat, 
            	    do.create_time,
            	    lead(do.create_time,1,do.create_time) over(partition by do.user_id order by do.create_time) create_time2,
	                lead(do.start_lng,1,0) over(PARTITION BY do.user_id ORDER BY do.create_time) start_lng2,
	                lead(do.start_lat,1,0) over(PARTITION BY do.user_id ORDER BY do.create_time) start_lat2,
	                lead(do.end_lng,1,0) over(PARTITION BY do.user_id ORDER BY do.create_time) end_lng2,
	                lead(do.end_lat,1,0) over(PARTITION BY do.user_id ORDER BY do.create_time) end_lat2,
	                lead(do.id,1,do.id) over(PARTITION BY do.user_id ORDER BY do.create_time) id2
        	    FROM (SELECT 
                        DISTINCT cast(get_json_object(event_values, '$.order_id') AS BIGINT) AS order_id
                    FROM oride_source.server_magic 
                    WHERE dt = '{pt}' AND 
                        event_name = 'dispatch_push_driver' AND 
                        get_json_object(event_values, '$.success') = 1 AND 
                        from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd') = '{pt}'
                    ) AS po  
        	    INNER JOIN (select * from oride_db.data_order where dt='{pt}') AS do 
        	    WHERE do.id = po.order_id AND 
        	        do.dt = '{pt}' AND  
            	    from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}'
    	        ) as t
    	    GROUP BY dt, city_id, serv_type 
        ),
        income_data as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
			    sum(nvl(amount_pay_online, 0)) as pay_online,           --线上支付
			    sum(nvl(amount_pay_offline, 0)) as pay_offline          --下线支付
		    from 
			    (select 
			        max(city_id) as city_id,
			        max(driver_serv_type) as serv_type,
			        max(dt) as dt,
				    driver_id 
			    from oride_db.data_order 
			    where dt = '{pt}' and 
				    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and 
				    status in (4,5)
				group by driver_id
			    ) as do 
		    left join (select * from oride_db.data_driver_records_day where dt='{pt}') as drd 
		    on drd.driver_id = do.driver_id 
		    where drd.dt = '{pt}' and 
			    from_unixtime(day, 'yyyy-MM-dd') = '{pt}' 
			group by do.dt, do.city_id, do.serv_type
        ),
        reward_data as (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
			    sum(if(isnotnull(rr.amount) and rr.amount>0, rr.amount, 0)) + sum(if(isnotnull(dr.amount) and dr.amount>0, dr.amount, 0)) as reward ---司机奖励
		    from 
			    (select 
			        max(city_id) as city_id,
			        max(driver_serv_type) as serv_type,
			        max(dt) as dt,
				    driver_id 
			    from oride_db.data_order 
			    where dt = '{pt}' and 
				    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and  
				    status in (4,5)
				group by driver_id 
			    ) as do 
		    left join (select * from oride_db.data_driver_recharge_records where dt='{pt}') as rr on do.driver_id = rr.driver_id 
		    left join (select * from oride_db.data_driver_reward where dt='{pt}') as dr on do.driver_id = dr.driver_id 
		    where rr.dt = '{pt}' and 
			    dr.dt = '{pt}' and 
			    from_unixtime(dr.create_time, 'yyyy-MM-dd') = '{pt}' and 
			    from_unixtime(rr.created_at, 'yyyy-MM-dd') = '{pt}' 
		    group by do.dt, do.city_id, do.serv_type
        ),
        driver_range as (
            select 
                do.city_id,
                do.driver_serv_type as serv_type,
                do.dt,
		    	sum(if(do.status=4, 
		        		abs(do.arrive_time-do.take_time), 
		        		if(do.status=5, 
		            		abs(do.finish_time-do.take_time),
		            		abs(do.cancel_time-do.take_time)
        				)
    			)) as do_range,                                     ---完单司机做单时长
			    count(distinct do.driver_id) as drivers_finished    ---完单司机数
		    from (select * from oride_db.data_order where dt='{pt}') as do 
		    inner join (select 
        			distinct driver_id 
    		    from oride_db.data_order 
    			where dt='{pt}' and 
        			from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' and   
        			status in (4,5)
    			) as off
		    where do.dt='{pt}' and 
    			do.driver_id = off.driver_id and 
    			do.status in (4,5,6) and 
    			from_unixtime(do.create_time, 'yyyy-MM-dd')='{pt}' 
		    group by do.dt, do.city_id, do.driver_serv_type
        ),
        driver_online as (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
	            sum(if(isnull(off.driver_id), 0, odt.driver_freerange)) as free_range_dr,           --司机空闲时长
	            sum(if(odt.driver_onlinerange>0, 1, 0)) as drvers_online                --在线司机数
            from (select * from oride_bi.oride_driver_timerange where dt='{pt}') as odt 
            left join (select 
		            distinct driver_id 
	            from oride_db.data_order 
	            where dt='{pt}' and 
		            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' and 
		            status in (4,5)
	            ) as off on off.driver_id = odt.driver_id 
            left join oride_db.data_driver_extend as de on odt.driver_id = de.id 
            where odt.dt = '{pt}' and 
                de.dt = '{pt}'
            group by de.dt, de.city_id, de.serv_type
        ) 
        insert overwrite table {hive_table} PARTITION (country_code='nal', dt='{pt}')
        select 
            orders_data.city_id,
            '',
            orders_data.serv_type,
            nvl(push_distance_finished.avg_distance, 0), --平均播单距离
            nvl(pick_distance_finished.avg_distance, 0), --平均接驾距离(完单)
            nvl(pick_distance_taked.avg_distance, 0), --平均接驾距离(应答)
            round(if(orders_show.avg_show>0, 
                nvl(click_data.avg_clicked, 0)/orders_show.avg_show, 0), 2), --调度服从率
            round(if(orders_data.orders>0, 
                nvl(pick_distance_taked.orders_clicked, 0)/orders_data.orders, 0), 2), --应答率
            nvl(orders_data.avg_take_range, 0),     --平均应答时长(完单)
            round(if(pick_distance_taked.orders_clicked>0, 
                nvl(orders_data.take_range, 0)/pick_distance_taked.orders_clicked, 0), 2), --平均应答时长(应答)   
            round(if(orders_data.orders>0, 
                nvl(orders_data.cancel_af_take_us, 0)/orders_data.orders, 0), 2),   --乘客应答后取消率                                            
            round(if(orders_data.orders>0, 
                nvl(orders_data.cancel_af_take_dr, 0)/orders_data.orders, 0), 2),     --司机应答后取消率
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.orders_f, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 0), 2),    --tph     
            round(if(driver_range.drivers_finished>0, 
                (nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))/driver_range.drivers_finished, 0), 2),    --平均在线时长
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.billing_range, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 0), 2),    --计费时长占比
            nvl(orders_data.orders, 0),                                            --下单数
            nvl(push_data.order_push, 0),                                       --播单数
            nvl(push_data.orders_pushed, 0),                                    --成功播单数
            round(if(orders_data.orders>0, 
                nvl(push_data.orders_pushed, 0)/orders_data.orders, 0), 2),     --播单率 
            nvl(orders_data.orders_take, 0),                                       --'接单数',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.orders_take,0)/nvl(orders_data.orders,0), 0), 2), --'接单率',
            round(if(nvl(push_data.orders_pushed, 0)>0, 
                nvl(orders_data.orders_take,0)/nvl(push_data.orders_pushed, 0), 0), 2), --'调度接单率',
            nvl(orders_data.orders_f, 0), --'完单数',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.orders_f, 0)/nvl(orders_data.orders,0), 0), 2), --'完单率',
            round(if(nvl(push_data.order_push, 0)>0, 
                nvl(orders_data.orders_f, 0)/nvl(push_data.order_push, 0), 0), 2),   --'调度完单率',
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.orders_f, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 0), 2), --'tph',
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.send_dis, 0)/nvl(orders_data.orders_f, 0), 0), 2), --'平均送驾距离', 
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.pick_range, 0)/nvl(orders_data.orders_f, 0), 0), 2), --'平均接驾时长',
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.billing_range,0)/nvl(orders_data.orders_f,0), 0), 2), --'平均计费时长',
            round(if(nvl(orders_data.payed_orders,0)>0, 
                nvl(pay_range,0)/nvl(orders_data.payed_orders,0), 0), 2), --'平均支付时长',
            nvl(driver_online.drvers_online, 0), --'在线司机数',
            nvl(click_data.drivers_clicked, 0), --'接单司机数',
            nvl(driver_range.drivers_finished, 0), --'完单司机数',
            round(if(nvl(driver_range.drivers_finished,0)>0, 
                nvl(orders_data.orders_f,0)/nvl(driver_range.drivers_finished,0), 0), 0), --'人均完单数',
            nvl(push_data.avg_order_pushed, 0), --'人均推送订单数',
            round(if(nvl(click_data.drivers_clicked,0)>0, 
                nvl(pick_distance_taked.orders_clicked,0)/nvl(click_data.drivers_clicked,0), 0), 0), --'人均应答订单数',            
            round(if(nvl(push_data.avg_order_pushed,0)>0, 
                round(if(nvl(click_data.drivers_clicked,0)>0, 
                    nvl(pick_distance_taked.orders_clicked,0)/nvl(click_data.drivers_clicked,0), 
                0), 0)/nvl(push_data.avg_order_pushed,0), 0), 2), --'司机服从率',
            round(if(driver_range.drivers_finished>0, 
                (nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))/driver_range.drivers_finished, 0), 2),    --'人均在线时长',    
            round(if(nvl(orders_data.orders_f,0)>0, 
                nvl(driver_range.do_range,0)/nvl(orders_data.orders_f,0), 0), 2), --'平均服务时长',            
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                (nvl(income_data.pay_online,0)+nvl(income_data.pay_offline,0)+nvl(reward_data.reward,0))/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 0), 2), --'完单司机IPH', 
            round(if(nvl(driver_range.drivers_finished,0)>0, 
                (nvl(income_data.pay_online,0)+nvl(income_data.pay_offline,0)+nvl(reward_data.reward,0))/nvl(driver_range.drivers_finished,0), 0), 2), --'完单司机日薪', 
            round(if(nvl(push_data.count_pushed,0)>0, 
                nvl(orders_show.click_count,0)/nvl(push_data.count_pushed,0), 0), 2),   --'播报到达率',
            round(if(nvl(push_data.drivers_pushed,0)>0, 
                nvl(push_data.count_pushed,0)/nvl(push_data.drivers_pushed,0), 0), 0), --'人均推单次数',
            round(if(nvl(dirvers_pushed.drivers_per_round,0)>0, 
                nvl(push_data.count_pushed,0)/nvl(dirvers_pushed.drivers_per_round,0), 0), 0), --'平均推送司机数',
            nvl(orders_data.cancel_all, 0),  --'取消订单数',
            nvl(orders_data.cancel_sys, 0),  --'系统取消订单数',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_sys,0)/nvl(orders_data.orders,0), 0), 2),  --'系统取消率',
            nvl(orders_data.cancel_bf_take_us, 0),  --'乘客应答前取消数',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_bf_take_us,0)/nvl(orders_data.orders,0), 0), 2),    --'乘客应答前取消率',
            nvl(orders_data.cancel_af_take_all, 0),  --'应答后取消数',
            nvl(orders_data.cancel_af_take_us, 0),  --'乘客应答后取消数',
            round(if(nvl(pick_distance_taked.orders_clicked,0)>0, 
                nvl(orders_data.cancel_af_take_us_range,0)/nvl(pick_distance_taked.orders_clicked,0)/60, 0), 2),    --'乘客应答后取消平均时长（分）',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_af_take_us,0)/nvl(orders_data.orders,0), 0), 2), --'乘客应答后取消率',
            nvl(pick_distance_finished.avg_distance_us_cancel, 0), --'乘客取消订单平均接驾距离',
            nvl(orders_data.cancel_af_take_dr, 0),  --'司机应答后取消数',
            round(if(nvl(pick_distance_taked.orders_clicked,0)>0, 
                nvl(orders_data.cancel_af_take_dr_range,0)/nvl(pick_distance_taked.orders_clicked,0)/60, 0), 2),    --'司机应答后取消平均时长（分）',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_af_take_dr,0)/nvl(orders_data.orders,0), 0), 2),    --'司机应答后取消率',
            nvl(click_data.click_count, 0),  --'司机应答总次数',
            nvl(click_data.avg_clicked, 0),  --'司机人均应答次数',
            nvl(valid_orders.valid_order, 0),  --'有效下单数',
            round(if(nvl(valid_orders.valid_order,0)>0, 
                nvl(orders_data.orders_f,0)/nvl(valid_orders.valid_order,0), 0), 2)    --'有效完单率'
        from push_distance_finished 
        full outer join pick_distance_finished 
            on push_distance_finished.dt=pick_distance_finished.dt and push_distance_finished.city_id=pick_distance_finished.city_id and push_distance_finished.serv_type=pick_distance_finished.serv_type 
        full outer join pick_distance_taked 
            on push_distance_finished.dt=pick_distance_taked.dt and push_distance_finished.city_id=pick_distance_taked.city_id and push_distance_finished.serv_type=pick_distance_taked.serv_type
        full outer join click_data 
            on push_distance_finished.dt=click_data.dt and push_distance_finished.city_id=click_data.city_id and push_distance_finished.serv_type=click_data.serv_type
        full outer join push_data 
            on push_distance_finished.dt=push_data.dt and push_distance_finished.city_id=push_data.city_id and push_distance_finished.serv_type=push_data.serv_type
        full outer join dirvers_pushed 
            on push_distance_finished.dt=dirvers_pushed.dt and push_distance_finished.city_id=dirvers_pushed.city_id and push_distance_finished.serv_type=dirvers_pushed.serv_type
        full outer join orders_show 
            on push_distance_finished.dt=orders_show.dt and push_distance_finished.city_id=orders_show.city_id and push_distance_finished.serv_type=orders_show.serv_type
        full outer join orders_data 
            on push_distance_finished.dt=orders_data.dt and push_distance_finished.city_id=orders_data.city_id and push_distance_finished.serv_type=orders_data.serv_type
        full outer join valid_orders 
            on push_distance_finished.dt=valid_orders.dt and push_distance_finished.city_id=valid_orders.city_id and push_distance_finished.serv_type=valid_orders.serv_type
        full outer join income_data 
            on push_distance_finished.dt=income_data.dt and push_distance_finished.city_id=income_data.city_id and push_distance_finished.serv_type=income_data.serv_type
        full outer join reward_data 
            on push_distance_finished.dt=reward_data.dt and push_distance_finished.city_id=reward_data.city_id and push_distance_finished.serv_type=reward_data.serv_type
        full outer join driver_range 
            on push_distance_finished.dt=driver_range.dt and push_distance_finished.city_id=driver_range.city_id and push_distance_finished.serv_type=driver_range.serv_type
        full outer join driver_online 
            on push_distance_finished.dt=driver_online.dt and push_distance_finished.city_id=driver_online.city_id and push_distance_finished.serv_type=driver_online.serv_type
        
    '''.format(
        pt='{{ ds }}', hive_table=hive_table
    ),
    schema='oride_dw',
    dag=dag
)


dependence_ods_oride_server_magic >> sleep_time
dependence_ods_oride_client_event >> sleep_time
dependence_ods_oride_data_order >> sleep_time
dependence_ods_oride_data_driver_records_day >> sleep_time
dependence_ods_oride_data_driver_recharge_records >> sleep_time
dependence_ods_oride_data_driver_reward >> sleep_time
dependence_ods_oride_driver_timerange >> sleep_time

sleep_time >> create_result_table_task
create_result_table_task >> drop_partitons_from_table >> insert_result_to_hive
