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
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
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
dependence_ods_oride_server_magic = S3PrefixSensor(
    task_id='dependence_ods_oride_server_magic',
    prefix='oride_buried/ordm.dispatch_tracker_server_magic/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

#dependence_ods_oride_server_magic = UFileSensor(
#    task_id='dependence_ods_oride_server_magic',
#    filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
#        hdfs_path_str="oride/server_magic",
#        pt='{{ ds }}'
#    ),
#    bucket_name='opay-datalake',
#    poke_interval=60,                                           #依赖不满足时，一分钟检查一次依赖状态
#    dag=dag
#)

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
            push_per_driver decimal(10,2) comment '人均推送订单数',
            take_per_driver decimal(10,2) comment '人均应答订单数',
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
        set mapred.job.queue.name=root.users.airflow;
        set hive.execution.engine=tez;
        set hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        set hive.vectorized.execution.enabled = true;
        set hive.vectorized.execution.reduce.enabled = true;
        set hive.prewarm.enabled=true;
        set hive.prewarm.numcontainers=16;
        set hive.tez.auto.reducer.parallelism=true;
        set hive.exec.parallel=true;
        WITH
        --推单距离 
        push_distance_finished as 
        (
            select 
                de.city_id,
                de.serv_type,
	            de.dt,
	            round(if(count(1)>0,
	                sum(sm.dis)/count(1), 
	                0), 
	            2) as avg_distance             --平均播单距离 
            from (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de  
            inner join (select 
                    cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id,
                    get_json_object(event_values, '$.distance') as dis 
                from oride_source.dispatch_tracker_server_magic 
                where dt='{pt}' and 
                    event_name = 'dispatch_push_driver' and 
	                get_json_object(event_values, '$.success') = 1 
                ) as sm  
            where sm.driver_id = de.id  
            group by de.dt, de.city_id, de.serv_type
        ), 
        --平均接驾距离(完单), 平均用户取消订单接驾距离
        pick_distance_finished as 
        (
            select 
                de.city_id,
                de.serv_type,
				de.dt,
				round(if(sum(if(o.status in (4, 5), 1, 0))>0, 
				    sum(if(o.status in (4, 5), m.dis, 0))/sum(if(o.status in (4, 5), 1, 0)), 
				    0), 
				2) as avg_distance,                                  --平均接驾距离(完单)
				round(if(sum(if(o.status=6 and o.cancel_role=1, 1, 0))>0, 
				    sum(if(o.status=6 and o.cancel_role=1, m.dis, 0))/sum(if(o.status=6 and o.cancel_role=1, 1, 0)), 
				    0), 
				2) as avg_distance_us_cancel                        --平均用户取消订单接驾距离
			from 
			    (select 
	    			*
    			from 
					(select 
						dt,
						split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''),']',''), ',') as drivers, 
						split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
						get_json_object(event_values, '$.order_id') as order_id
					from oride_source.dispatch_tracker_server_magic 
					where dt='{pt}' and 
						event_name='dispatch_assign_driver' and 
		    			from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
	    			) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id 
	    			lateral view posexplode(distances) ds as dspos, dis 
	    			where dpos = dspos
    			) as m 
    		inner join (select 
    			    id, 
    			    status, 
    			    cancel_role
    			from oride_db.data_order 
    			where dt='{pt}' and 
    			    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
    			) as o 
    		inner join (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de 
			where 
			    cast(m.order_id as bigint) = o.id and 
			    cast(m.driver_id as bigint) = de.id 
			group by de.dt, de.city_id, de.serv_type
        ),
        --平均接驾距离(应答)
        pick_distance_taked as 
        (
            select 
                de.city_id,
                de.serv_type,
    			de.dt,
    			round(if(count(1)>0, 
    			    sum(m.dis)/count(1), 
    			    0), 
    			2) as avg_distance                                 --平均接驾距离(应答)
		    from 
    			(select 
        			*
    			from 
        			(select 
            			dt,
        				split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''), ']',''), ',') as drivers, 
        				split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
        				get_json_object(event_values, '$.order_id') as order_id
    				from oride_source.dispatch_tracker_server_magic 
				    where dt='{pt}' and 
					    event_name='dispatch_assign_driver' 
				    ) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id 
				    lateral view posexplode(distances) ds as dspos, dis 
				    where dpos = dspos
	    		) as m 
	    	inner join (select 
                    distinct get_json_object(event_value, '$.order_id') as id 
                from oride_bi.oride_client_event_detail 
                where dt='{pt}' and 
                    event_name = 'accept_order_click'
            	) as o on cast(m.order_id as bigint) = cast(o.id as bigint) 
            inner join (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as  de on cast(m.driver_id as bigint) = de.id  
		    group by de.dt, de.city_id, de.serv_type
        ),
        --sum(每个司机应答的订单数去重)
        sum_driver_click_orders as 
        (
            select 
                de.city_id,
                de.serv_type,
    			de.dt,
    			sum(orders) as orders_clicked_all                   --sum(每个司机应答的订单数去重)
		    from 
    			(select 
                    count(distinct get_json_object(event_value, '$.order_id')) as orders, 
                    user_id 
                from oride_bi.oride_client_event_detail 
                where dt='{pt}' and 
                    event_name = 'accept_order_click' 
                group by user_id
            	) as o  
            inner join (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de on cast(o.user_id as bigint) = de.id  
		    group by de.dt, de.city_id, de.serv_type    
        ),
        --司机应答总次数, 应答司机数, 人均应答次数
        click_data as 
        (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            count(1) as click_count,                                        --司机应答总次数sum(每个司机应答的次数)
	            count(distinct ce.user_id) as drivers_clicked,                  --应答司机数
	            if(count(distinct ce.user_id)>0, 
	                count(1)/count(distinct ce.user_id), 
	                0
	            ) as avg_clicked                                                --人均应答次数
            from
                (select 
                    cast(user_id as bigint) as user_id
                from oride_bi.oride_client_event_detail 
                where dt = '{pt}' and 
                    event_name = 'accept_order_click'
                ) as ce 
            inner join (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as do  
            where ce.user_id = do.id  
            group by do.dt, do.city_id, do.serv_type
        ),
        --成功推送司机数, 推送给司机的总次数(成功), 推送给司机的总次数(全部)
        push_data as 
        (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            count(distinct if(success=1, t.driver_id, null)) as drivers_pushed,     --成功推送司机数
	            sum(if(success=1, 1, 0)) as count_pushed,                               --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
	            count(1) as count_pushed_all,                                           --推送给司机的总次数(全部)
	            if(count(distinct if(success=1, t.driver_id, null))>0, 
	                sum(if(success=1, 1, 0))/count(distinct if(success=1, t.driver_id, null)), 
	            0) as push_count_per                                                    --人均推单次数
            from (
                select 
                    cast(get_json_object(event_values, '$.success') as int) as success,
                    cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id
                from oride_source.dispatch_tracker_server_magic 
                where dt='{pt}' and 
	                event_name='dispatch_push_driver' and  
	                from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
	            ) as t 
            inner join (select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as do  
            where t.driver_id = do.id  
            group by do.dt, do.city_id, do.serv_type
        ),
        --推单数(分业务)
        push_data_chose as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
                count(distinct m.order_id) as order_push            --推单数
            from (select 
        			*
    			from 
        			(select 
        				split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''), ']',''), ',') as drivers, 
        				get_json_object(event_values, '$.order_id') as order_id
    				from oride_source.dispatch_tracker_server_magic 
				    where dt='{pt}' and 
					    event_name='dispatch_chose_driver' 
				    ) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id  
	    		) as m 
	    	inner join (
	    	    select 
                    id, 
                    serv_type, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
	    	    ) as de 
	    	where cast(m.driver_id as bigint) = de.id 
	    	group by de.dt, de.city_id, de.serv_type
        ),
        --推单数(不分业务)
        push_data_chose_nobs as 
        (
            select 
                de.city_id,
                -1 as serv_type,
                de.dt,
                count(distinct m.order_id) as order_push            --推单数
            from (select 
        			*
    			from 
        			(select 
        				split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''), ']',''), ',') as drivers, 
        				get_json_object(event_values, '$.order_id') as order_id
    				from oride_source.dispatch_tracker_server_magic 
				    where dt='{pt}' and 
					    event_name='dispatch_chose_driver' 
				    ) as t 
				    lateral view posexplode(drivers) d as dpos, driver_id  
	    		) as m 
	    	inner join (
	    	    select 
                    id, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
	    	    ) as de 
	    	where cast(m.driver_id as bigint) = de.id 
	    	group by de.dt, de.city_id
        ),
        --成功推送订单数
        push_data_success as 
        (
            select 
                de.city_id,
                -1 as serv_type,
                de.dt,
                count(distinct t.order_id) as orders_pushed      --成功推送订单数
            from (select 
                    cast(get_json_object(event_values, '$.order_id') as bigint) as order_id,
                    cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id
                from oride_source.dispatch_tracker_server_magic 
                where dt='{pt}' and 
	                event_name='dispatch_push_driver' and  
	                from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}' and 
	                cast(get_json_object(event_values, '$.success') as int) = 1
	            ) as t 
	        inner join (select 
                    id, 
                    city_id, 
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de  
            where cast(t.driver_id as bigint) = de.id 
            group by de.dt, de.city_id
        ),
        --SUM(每个订单每轮被推送的司机数去重)
        dirvers_pushed as 
        (
            select 
                t.city_id,
                t.serv_type,
                t.dt,
	            sum(drivers) as drivers_per_round   --SUM(每个订单每轮被推送的司机数去重)
            from 
                (select 
                    do.city_id,
                    do.serv_type,
                    do.dt,
		            count(distinct sm.driver_id) as drivers
                from (select 
                        cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id,
                        cast(get_json_object(event_values, '$.order_id') as bigint) as order_id,
                        get_json_object(event_values, '$.round') as smround
                    from oride_source.dispatch_tracker_server_magic 
                    where dt='{pt}' and 
                        event_name = 'dispatch_push_driver' and 
		                from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd') = '{pt}' and 
		                get_json_object(event_values, '$.success') = 1
                    ) as sm 
                inner join (select 
                        id, 
                        city_id, 
                        serv_type,
                        dt 
                    from oride_db.data_driver_extend 
                    where dt='{pt}'
                    ) as do  
                where cast(sm.driver_id as bigint) = do.id  
                group by do.dt, do.city_id, do.serv_type, sm.order_id, sm.smround
                ) as t 
            group by t.dt, t.city_id, t.serv_type
        ),
        --订单show数据
        orders_show as 
        (
            select 
                do.city_id,
                do.serv_type,
                do.dt,
	            sum(show_count) as show_count,                          --订单推送展示次数(推送成功次数)
                count(distinct t.user_id) as drivers_show,              --展示司机数（成功推送司机数）
                if(count(distinct t.user_id)>0, 
                    sum(show_count)/count(distinct t.user_id), 
                    0
                ) as avg_show,                                          --人均展示次数
                sum(order_id) as show_orders                            --成功推送总订单数,sum(每个司机被推送的订单数去重)
            from (
                select 
                    count(distinct cast(get_json_object(event_value, '$.order_id') as bigint)) as order_id,
                    user_id,
                    count(1) as show_count
                from oride_bi.oride_client_event_detail 
                where dt='{pt}' and 
                    event_name = 'accept_order_show'
                group by user_id
                ) as t 
            inner join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as do  
            where cast(t.user_id as bigint) = do.id  
            group by do.dt, do.city_id, do.serv_type
        ),
        --订单数据(不分业务)
        orders_data_nobs as 
        (
            select 
                city_id,
                -1 as serv_type,
                dt,
    			count(1) as orders,                                                                                         --下单数
    			sum(if(driver_id>0, 1, 0)) as orders_take,                                                                  --有司机接单数
			    sum(if(status in (4,5), abs(take_time-create_time), 0)) as take_range,                                      --总应答时长（完单秒）
    			sum(if(status in (4,5), 1, 0)) as orders_f,                                                                 --完成订单数
    			sum(if(status in (4,5), abs(take_time-create_time), 0))/sum(if(status in (4,5), 1, 0)) as avg_take_range,   --平均应答时长(完单秒)
			    sum(if(status=6 and driver_id>0 and cancel_role=1, 1, 0)) as cancel_af_take_us,                             --乘客应答后取消订单数
			    sum(if(status=6 and driver_id>0 and cancel_role=2, 1, 0)) as cancel_af_take_dr,                             --司机应答后取消订单数
			    sum(if(status=6, 1, 0)) as cancel_all,                                                                      --总取消订单数
			    sum(if(status=6 and cancel_role in (3,4), 1, 0)) as cancel_sys,                                             --系统总取消订单数
			    sum(if(status=6 and driver_id=0 and cancel_role=1, 1, 0)) as cancel_bf_take_us,                             --乘客应答前取消订单数
			    sum(if(status=6 and driver_id>0, 1, 0)) as cancel_af_take_all,                                              --应答后取消订单总数
			    sum(if(status=6 and driver_id>0 and cancel_role=1, abs(cancel_time-take_time), 0)) as cancel_af_take_us_range, --乘客应答后取消总时长(秒)
			    sum(if(status=6 and driver_id>0 and cancel_role=2, abs(cancel_time-take_time), 0)) as cancel_af_take_dr_range, --司机应答后取消总时长(秒)
			    sum(if(status in (4,5), abs(arrive_time-pickup_time), 0)) as billing_range,                                 --总计费时长/总服务时长(秒)
			    sum(if(status in (4,5), abs(pickup_time - take_time), 0)) as pick_range,                                    --总接驾时长(秒)
			    sum(if(status in (4,5), distance, 0)) as send_dis,                                                          --总送驾距离(米)
			    sum(if(status=5, abs(finish_time-arrive_time), 0)) as pay_range,                                            --总支付时长(秒)
			    sum(if(status=5, 1, 0)) as payed_orders                                                                     --支付订单数
		    from oride_db.data_order 
		    where dt='{pt}' and 
    			from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
    		group by dt, city_id 
        ),
        --订单数据(分业务)
        orders_data as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
                0 as orders,
    			sum(if(o.driver_id>0, 1, 0)) as orders_take,                                                                            --有司机接单数
			    sum(if(o.status in (4,5), abs(o.take_time-o.create_time), 0)) as take_range,                                            --总应答时长（完单秒）
    			sum(if(o.status in (4,5), 1, 0)) as orders_f,                                                                           --完成订单数
    			sum(if(o.status in (4,5), abs(o.take_time-o.create_time), 0))/sum(if(o.status in (4,5), 1, 0)) as avg_take_range,       --平均应答时长(完单秒)
			    sum(if(o.status=6 and o.driver_id>0 and o.cancel_role=1, 1, 0)) as cancel_af_take_us,                                   --乘客应答后取消订单数
			    sum(if(o.status=6 and o.driver_id>0 and o.cancel_role=2, 1, 0)) as cancel_af_take_dr,                                   --司机应答后取消订单数
			    0 as cancel_all,
			    sum(if(o.status=6 and o.driver_id>0 and o.cancel_role in (3,4), 1, 0)) as cancel_sys,                                   --系统总取消订单数
			    0 as cancel_bf_take_us,
			    sum(if(o.status=6 and o.driver_id>0, 1, 0)) as cancel_af_take_all,                                                      --应答后取消订单总数
			    sum(if(o.status=6 and o.driver_id>0 and o.cancel_role=1, abs(o.cancel_time-take_time), 0)) as cancel_af_take_us_range,  --乘客应答后取消总时长(秒)
			    sum(if(o.status=6 and o.driver_id>0 and o.cancel_role=2, abs(o.cancel_time-take_time), 0)) as cancel_af_take_dr_range,  --司机应答后取消总时长(秒)
			    sum(if(o.status in (4,5), abs(o.arrive_time-o.pickup_time), 0)) as billing_range,                                       --总计费时长/总服务时长(秒)
			    sum(if(o.status in (4,5), abs(o.pickup_time - o.take_time), 0)) as pick_range,                                          --总接驾时长(秒)
			    sum(if(o.status in (4,5), distance, 0)) as send_dis,                                                                    --总送驾距离(米)
			    sum(if(o.status=5, abs(o.finish_time-o.arrive_time), 0)) as pay_range,                                                  --总支付时长(秒)
			    sum(if(o.status=5, 1, 0)) as payed_orders                                                                               --支付订单数
		    from (select 
		            driver_id,
		            status,
		            take_time,
		            create_time,
		            cancel_role,
		            pickup_time,
		            arrive_time,
		            distance,
		            cancel_time,
		            finish_time 
		        from oride_db.data_order 
		        where dt='{pt}' and 
    			    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and 
    			    driver_id > 0
    			) as o 
    		inner join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de  
		    where o.driver_id = de.id
    		group by de.dt, de.city_id, de.serv_type 
        ),
        --有效下单数
        valid_orders as 
        (
            SELECT 
                city_id,
                -1 as serv_type,
                dt,
        	    sum(if(status in (4,5), 1, 
        	        if(id=id2, 1, 
        	            if(abs(create_time2-create_time)<=1800 and 
        	                2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 and 
        	                2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
        	            )
        	        )
        	    )) as valid_order
    	    FROM (
    	        SELECT 
                    city_id,
                    dt,
            	    id, 
            	    user_id, 
            	    start_lng, 
            	    start_lat, 
            	    end_lng, 
            	    end_lat, 
            	    create_time,
            	    status,
            	    lead(create_time,1,create_time) over(partition by user_id order by create_time) create_time2,
	                lead(start_lng,1,0) over(PARTITION BY user_id ORDER BY create_time) start_lng2,
	                lead(start_lat,1,0) over(PARTITION BY user_id ORDER BY create_time) start_lat2,
	                lead(end_lng,1,0) over(PARTITION BY user_id ORDER BY create_time) end_lng2,
	                lead(end_lat,1,0) over(PARTITION BY user_id ORDER BY create_time) end_lat2,
	                lead(id,1,id) over(PARTITION BY user_id ORDER BY create_time) id2
        	    FROM oride_db.data_order 
        	    where dt='{pt}' and 
        	        from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' 
    	        ) as t
    	    GROUP BY dt, city_id 
        ),
        --司机收入
        income_data as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
			    sum(nvl(drd.amount_pay_online, 0)) as pay_online,           --线上支付
			    sum(nvl(drd.amount_pay_offline, 0)) as pay_offline          --下线支付
		    from 
			    (select 
				    driver_id 
			    from oride_db.data_order 
			    where dt = '{pt}' and 
				    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and 
				    status in (4,5)
				group by driver_id
			    ) as do 
			left join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de on de.id = do.driver_id 
		    left join (select 
		            amount_pay_online,
		            amount_pay_offline,
		            driver_id,
		            dt
		        from oride_db.data_driver_records_day 
		        where dt='{pt}' and 
		            from_unixtime(day, 'yyyy-MM-dd') = '{pt}'
		        ) as drd on drd.driver_id = do.driver_id  
			group by de.dt, de.city_id, de.serv_type
        ),
        --司机奖励
        reward_data as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
			    sum(if(isnotnull(rr.amount) and rr.amount>0, rr.amount, 0)) + sum(if(isnotnull(dr.amount) and dr.amount>0, dr.amount, 0)) as reward ---司机奖励
		    from 
			    (select 
				    driver_id 
			    from oride_db.data_order 
			    where dt = '{pt}' and 
				    from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and  
				    status in (4,5)
				group by driver_id 
			    ) as do 
			left join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de on de.id = do.driver_id 
		    left join (select 
		            amount,
		            driver_id
		        from oride_db.data_driver_recharge_records 
		        where dt='{pt}' and 
		            from_unixtime(created_at, 'yyyy-MM-dd') = '{pt}'
		        ) as rr on do.driver_id = rr.driver_id 
		    left join (select 
		            amount,
		            driver_id  
		        from oride_db.data_driver_reward 
		        where dt='{pt}' and 
		            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
		        ) as dr on do.driver_id = dr.driver_id 
		    group by de.dt, de.city_id, de.serv_type
        ),
        --司机做单时长
        driver_range as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
		    	sum(if(do.status=4, 
		        		abs(do.arrive_time-do.take_time), 
		        		if(do.status=5, 
		            		abs(do.finish_time-do.take_time),
		            		abs(do.cancel_time-do.take_time)
        				)
    			)) as do_range,                                                 ---完单司机做单时长
			    count(distinct do.driver_id) as drivers_finished                ---完单司机数
		    from (select 
		            status,
		            take_time,
		            arrive_time,
		            finish_time,
		            cancel_time,
		            driver_id
		        from oride_db.data_order 
		        where dt='{pt}' and 
		            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' and 
		            status in (4,5,6) 
		        ) as do 
		    inner join (select 
        			distinct driver_id 
    		    from oride_db.data_order 
    			where dt='{pt}' and 
        			from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' and   
        			status in (4,5)
    			) as off on do.driver_id = off.driver_id 
    		inner join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
    		    ) as de on de.id = do.driver_id 
		    group by de.dt, de.city_id, de.serv_type
        ),
        --司机空闲时长
        driver_online as 
        (
            select 
                de.city_id,
                de.serv_type,
                de.dt,
	            sum(if(isnull(off.driver_id), 0, odt.driver_freerange)) as free_range_dr,           --司机空闲时长
	            sum(if(odt.driver_onlinerange>0, 1, 0)) as drvers_online                            --在线司机数
            from (select 
                    driver_onlinerange,
                    driver_freerange,
                    driver_id 
                from oride_bi.oride_driver_timerange 
                where dt='{pt}'
                ) as odt 
            inner join (select 
		            distinct driver_id 
	            from oride_db.data_order 
	            where dt='{pt}' and 
		            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' and 
		            status in (4,5)
	            ) as off on off.driver_id = odt.driver_id 
            inner join (select 
                    id, 
                    city_id, 
                    serv_type,
                    dt 
                from oride_db.data_driver_extend 
                where dt='{pt}'
                ) as de on odt.driver_id = de.id 
            group by de.dt, de.city_id, de.serv_type
        ) 
        ---写入结果表
        insert overwrite table {hive_table} PARTITION (country_code='nal', dt='{pt}')
        select 
            --城市ID
            nvl(orders_data.city_id, 0),                                     
            --城市名
            '',                                                                                     
            --业务类型
            nvl(orders_data.serv_type, 0),                                 
            --平均播单距离
            nvl(push_distance_finished.avg_distance, 0.00),                                            
            --平均接驾距离(完单)
            nvl(pick_distance_finished.avg_distance, 0.00),                                            
            --平均接驾距离(应答)
            nvl(pick_distance_taked.avg_distance, 0.00),                                               
            --调度服从率
            round(if(push_data.push_count_per>0, 
                nvl(click_data.avg_clicked, 0)/push_data.push_count_per, 
                0), 
            2),                                                                                     
            --应答率
            round(if(orders_data.orders>0, 
                nvl(orders_data.orders_take, 0)/orders_data.orders, 
                0), 
            2),                                                                                     
            --平均应答时长(完单分)                
            round(nvl(orders_data.avg_take_range, 0)/60, 2),                                                     
            --平均应答时长(应答分)
            round(if(orders_data.orders_take>0, 
                nvl(orders_data.take_range, 0)/orders_data.orders_take/60, 
                0), 
            2),                                                                                     
            --乘客应答后取消率       
            round(if(orders_data.orders>0, 
                nvl(orders_data.cancel_af_take_us, 0)/orders_data.orders, 
                0), 
            2),                                                                                     
            --司机应答后取消率                                                        
            round(if(orders_data.orders>0, 
                nvl(orders_data.cancel_af_take_dr, 0)/orders_data.orders, 
                0), 
            2),                                                                                     
            --tph    
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.orders_f, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 
                0), 
            2),                                                                                     
            --平均在线时长(时)     
            round(if(driver_range.drivers_finished>0, 
                (nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))/driver_range.drivers_finished/3600, 
                0),
            2),                                                                                     
            --计费时长占比
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.billing_range, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 
                0), 
            2),                                                                                     
            --下单数    
            nvl(orders_data.orders, 0),                                                        
            --播单数
            nvl(push_data_chose.order_push, push_data_chose_nobs.order_push),                       
            --成功播单数
            nvl(push_data_success.orders_pushed, 0),                                                
            --播单率
            round(if(orders_data.orders>0, 
                nvl(push_data_success.orders_pushed, 0)/orders_data.orders, 
                0), 
            2),                                                                                     
            --'接单数',
            nvl(orders_data.orders_take, 0),
            --'接单率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.orders_take,0)/nvl(orders_data.orders,0), 
                0), 
            2),
            --'调度接单率',
            round(if(nvl(push_data_success.orders_pushed, 0)>0, 
                nvl(orders_data.orders_take,0)/nvl(push_data_success.orders_pushed, 0), 
                0), 
            2),
            --'完单数',
            nvl(orders_data.orders_f, 0),
            --'完单率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.orders_f, 0)/nvl(orders_data.orders,0), 
                0), 
            2),
            --'调度完单率',
            round(if(nvl(push_data_chose.order_push, 0)>0, 
                nvl(orders_data.orders_f, 0)/nvl(push_data_chose.order_push, 0), 
                0), 
            2),   
            --'tph',
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                nvl(orders_data.orders_f, 0)/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 
                0), 
            2),
            --'平均送驾距离', 
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.send_dis, 0)/nvl(orders_data.orders_f, 0), 
                0), 
            2), 
            --'平均接驾时长'(分), 
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.pick_range, 0)/nvl(orders_data.orders_f, 0)/60, 
                0), 
            2), 
            --'平均计费时长'(分),
            round(if(nvl(orders_data.orders_f, 0)>0, 
                nvl(orders_data.billing_range,0)/nvl(orders_data.orders_f,0)/60, 
                0), 
            2), 
            --'平均支付时长'(分),
            round(if(nvl(orders_data.payed_orders,0)>0, 
                nvl(orders_data.pay_range,0)/nvl(orders_data.payed_orders,0)/60, 
                0), 
            2),
            --'在线司机数',
            nvl(driver_online.drvers_online, 0),
            --'接单司机数',
            nvl(click_data.drivers_clicked, 0),
            --'完单司机数', 
            nvl(driver_range.drivers_finished, 0), 
            --'人均完单数',
            round(if(nvl(driver_range.drivers_finished,0)>0, 
                nvl(orders_data.orders_f,0)/nvl(driver_range.drivers_finished,0), 
                0), 
            0), 
            --'人均推送订单数',
            round(if(nvl(orders_show.drivers_show,0)>0, 
                nvl(orders_show.show_orders,0)/nvl(orders_show.drivers_show,0), 
                0), 
            2), 
            --'人均应答订单数',
            round(if(nvl(click_data.drivers_clicked,0)>0, 
                nvl(sum_driver_click_orders.orders_clicked_all,0)/nvl(click_data.drivers_clicked,0), 
                0), 
            2), 
            --'司机服从率',            
            round(if(nvl(orders_show.drivers_show,0)>0 and nvl(orders_show.show_orders,0)/nvl(orders_show.drivers_show,0)>0 and nvl(click_data.drivers_clicked,0)>0, 
                nvl(sum_driver_click_orders.orders_clicked_all,0)/nvl(click_data.drivers_clicked,0)/nvl(orders_show.show_orders,0)/nvl(orders_show.drivers_show,0), 
                0), 
            2), 
            --'人均在线时长'(时),
            round(if(driver_range.drivers_finished>0, 
                (nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))/driver_range.drivers_finished/3600, 
                0),
            2),    
            --'平均服务时长',    
            round(if(nvl(orders_data.orders_f,0)>0, 
                nvl(driver_range.do_range,0)/nvl(orders_data.orders_f,0), 
                0), 
            2), 
            --'完单司机IPH',         
            round(if((nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0))>0, 
                (nvl(income_data.pay_online,0)+nvl(income_data.pay_offline,0)+nvl(reward_data.reward,0))/(nvl(driver_range.do_range,0)+nvl(driver_online.free_range_dr,0)), 
                0), 
            2), 
            --'完单司机日薪', 
            round(if(nvl(driver_range.drivers_finished,0)>0, 
                (nvl(income_data.pay_online,0)+nvl(income_data.pay_offline,0)+nvl(reward_data.reward,0))/nvl(driver_range.drivers_finished,0), 
                0), 
            2), 
            --'播报到达率', 
            round(if(nvl(push_data.count_pushed_all,0)>0, 
                nvl(orders_show.show_count,0)/nvl(push_data.count_pushed_all,0), 
                0), 
            2),   
            --'人均推单次数',
            round(if(nvl(push_data.drivers_pushed,0)>0, 
                nvl(push_data.count_pushed,0)/nvl(push_data.drivers_pushed,0), 
                0), 
            0), 
            --'平均推送司机数',
            round(if(nvl(dirvers_pushed.drivers_per_round,0)>0, 
                nvl(push_data.count_pushed,0)/nvl(dirvers_pushed.drivers_per_round,0), 
                0), 
            0), 
            --'取消订单数',
            nvl(orders_data.cancel_all, 0),  
            --'系统取消订单数',
            nvl(orders_data.cancel_sys, 0),  
            --'系统取消率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_sys,0)/nvl(orders_data.orders,0), 
                0), 
            2),  
            --'乘客应答前取消数',
            nvl(orders_data.cancel_bf_take_us, 0),
            --'乘客应答前取消率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_bf_take_us,0)/nvl(orders_data.orders,0), 
                0), 
            2),    
            --'应答后取消数',    
            nvl(orders_data.cancel_af_take_all, 0),  
            --'乘客应答后取消数',
            nvl(orders_data.cancel_af_take_us, 0),  
            --'乘客应答后取消平均时长（分）',
            round(if(nvl(orders_data.orders_take,0)>0, 
                nvl(orders_data.cancel_af_take_us_range,0)/nvl(orders_data.orders_take,0)/60, 
                0), 
            2),    
            --'乘客应答后取消率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_af_take_us,0)/nvl(orders_data.orders,0), 
                0), 
            2), 
            --'乘客取消订单平均接驾距离',    
            nvl(pick_distance_finished.avg_distance_us_cancel, 0), 
            --'司机应答后取消数',
            nvl(orders_data.cancel_af_take_dr, 0),  
            --'司机应答后取消平均时长（分）',
            round(if(nvl(orders_data.orders_take,0)>0, 
                nvl(orders_data.cancel_af_take_dr_range,0)/nvl(orders_data.orders_take,0)/60, 
                0), 
            2),    
            --'司机应答后取消率',
            round(if(nvl(orders_data.orders,0)>0, 
                nvl(orders_data.cancel_af_take_dr,0)/nvl(orders_data.orders,0), 
                0), 
            2),    
            --'司机应答总次数',
            nvl(click_data.click_count, 0),
            --'司机人均应答次数',
            nvl(click_data.avg_clicked, 0),  
            --'有效下单数',
            nvl(valid_orders.valid_order, 0),  
            --'有效完单率'
            round(if(nvl(valid_orders.valid_order,0)>0, 
                nvl(orders_data.orders_f,0)/nvl(valid_orders.valid_order,0), 
                0), 
            2)    
                
        from (
            select * from orders_data union select * from orders_data_nobs
            ) as orders_data 
        left join push_distance_finished 
            on orders_data.dt = push_distance_finished.dt and 
                orders_data.city_id = push_distance_finished.city_id and 
                orders_data.serv_type = push_distance_finished.serv_type 
        left join pick_distance_finished 
            on orders_data.dt=pick_distance_finished.dt and 
                orders_data.city_id=pick_distance_finished.city_id and 
                orders_data.serv_type=pick_distance_finished.serv_type 
        left join pick_distance_taked 
            on orders_data.dt=pick_distance_taked.dt and 
                orders_data.city_id=pick_distance_taked.city_id and 
                orders_data.serv_type=pick_distance_taked.serv_type 
        left join sum_driver_click_orders 
            on orders_data.dt=sum_driver_click_orders.dt and 
                orders_data.city_id=sum_driver_click_orders.city_id and 
                orders_data.serv_type=sum_driver_click_orders.serv_type 
        left join click_data 
            on orders_data.dt=click_data.dt and 
                orders_data.city_id=click_data.city_id and 
                orders_data.serv_type=click_data.serv_type
        left join push_data 
            on orders_data.dt=push_data.dt and 
                orders_data.city_id=push_data.city_id and 
                orders_data.serv_type=push_data.serv_type 
        left join push_data_chose 
            on orders_data.dt=push_data_chose.dt and 
                orders_data.city_id=push_data_chose.city_id and 
                orders_data.serv_type=push_data_chose.serv_type 
        left join push_data_chose_nobs 
            on orders_data.dt=push_data_chose_nobs.dt and 
                orders_data.city_id=push_data_chose_nobs.city_id and 
                orders_data.serv_type=push_data_chose_nobs.serv_type 
        left join push_data_success 
            on orders_data.dt=push_data_success.dt and 
                orders_data.city_id=push_data_success.city_id and 
                orders_data.serv_type=push_data_success.serv_type 
        left join dirvers_pushed 
            on orders_data.dt=dirvers_pushed.dt and 
                orders_data.city_id=dirvers_pushed.city_id and 
                orders_data.serv_type=dirvers_pushed.serv_type
        left join orders_show 
            on orders_data.dt=orders_show.dt and 
                orders_data.city_id=orders_show.city_id and 
                orders_data.serv_type=orders_show.serv_type 
        left join valid_orders 
            on orders_data.dt=valid_orders.dt and 
                orders_data.city_id=valid_orders.city_id and 
                orders_data.serv_type=valid_orders.serv_type
        left join income_data 
            on orders_data.dt=income_data.dt and 
                orders_data.city_id=income_data.city_id and 
                orders_data.serv_type=income_data.serv_type
        left join reward_data 
            on orders_data.dt=reward_data.dt and 
                orders_data.city_id=reward_data.city_id and 
                orders_data.serv_type=reward_data.serv_type
        left join driver_range 
            on orders_data.dt=driver_range.dt and 
                orders_data.city_id=driver_range.city_id and 
                orders_data.serv_type=driver_range.serv_type
        left join driver_online 
            on orders_data.dt=driver_online.dt and 
                orders_data.city_id=driver_online.city_id and 
                orders_data.serv_type=driver_online.serv_type
        
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
