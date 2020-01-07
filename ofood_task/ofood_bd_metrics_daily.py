# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.utils.email import send_email
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
import codecs
import csv
import logging

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 9, 23),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'ofood_bd_metrics_daily',
    schedule_interval="30 04 * * *",
    default_args=args)

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names":
            [
                'ofood_dw_ods.ods_sqoop_base_jh_order_df',
                'ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df',
                'ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df',
                'ofood_dw_ods.ods_sqoop_bd_invitation_info_df',
                'ofood_dw_ods.ods_sqoop_bd_jh_member_df',

            ],
        # 任务名称
        "task_name": "ofood BD指标"
    },
    dag=dag
)

# 熔断阻塞流程
jh_order_validate_task = HivePartitionSensor(
    task_id="jh_order_validate_task",
    table="ods_sqoop_base_jh_order_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_admin_users_validate_task = HivePartitionSensor(
    task_id="bd_admin_users_validate_task",
    table="ods_sqoop_bd_bd_admin_users_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_bd_fence_validate_task = HivePartitionSensor(
    task_id="bd_bd_fence_validate_task",
    table="ods_sqoop_bd_bd_bd_fence_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_invitation_info_validate_task = HivePartitionSensor(
    task_id="bd_invitation_info_validate_task",
    table="ods_sqoop_bd_invitation_info_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

create_bd_data = BashOperator(
    task_id='create_bd_data',
    bash_command="""
        
        dt="{{ ds }}"
        ds="{{ ds_nodash }}"
        
        bd_sql="
        
            create temporary function isInArea as 'com.oride.udf.IsInArea' 
            USING JAR 'oss://opay-datalake/udf-1.0-SNAPSHOT-jar-with-dependencies.jar';
            
            set hive.strict.checks.cartesian.product=false;
            set hive.mapred.mode=nonstrict;
            set hive.auto.convert.join = false;
        
            with 
            bd_area as (
                select 
                a.id bd_id,
                a.name,
                b.id area_id,
                b.area_name,
                b.points
                from ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df b
                join ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df a on a.dt = '${dt}' and b.uid = a.id and job_id = 4
                where b.dt = '${dt}'
            
            ),
            
            
            order_data as (
                select 
                b.bd_id,
                b.name,
                b.area_id,
                b.area_name,
                
                nvl(count(o.order_id),0) order_num,
                nvl(count(if(o.order_status = 8 and o.shop_id is not null,o.order_id,null)),0) success_order_num,
                nvl(count(if(o.pay_status = 1 and o.shop_id is not null,o.order_id,null)),0) pay_order_num,
                nvl(count(if(o.pay_status = 0,o.order_id,null)),0) before_pay_cancel_num,
                nvl(count(if(o.order_status = -2,o.order_id,null)),0) after_pay_cancel_num,
                nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null, d.first_roof + d.roof_mj + d.roof_delivery + d.roof_capped ,0)),0) cost_price_sum,
                nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null,d.origin_product + d.origin_package + d.origin_delivery,0)),0) trade_price_sum,
                nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null,d.origin_product + d.origin_package + d.origin_delivery - o.order_youhui - o.first_youhui,0)),0) actual_trade_price_sum,
                nvl(sum(if(o.order_status = 8 and o.shop_id is not null,o.amount,0)),0) amount_price_sum
                
                from 
                bd_area b 
                left join ofood_dw_ods.ods_sqoop_base_jh_order_df o on o.dt = '${dt}' and o.day = '${ds}'
                left join ofood_dw_ods.ods_sqoop_base_jh_waimai_order_df d on d.dt = '${dt}' and o.order_id = d.order_id
                where isInArea(b.points,o.o_lat,o.o_lng) = 1
                group by b.bd_id,b.name,b.area_id,b.area_name
            ),
    
    
            new_data as 
            (
            
                select
                b.bd_id,
                b.name,
                b.area_id,
                b.area_name,
                count(distinct(o.uid)) as new_user_complete_num
                
                from 
                bd_area b 
                left join 
                (
                    select 
                    o.order_status,
                    o.o_lat,
                    o.o_lng,
                    o.dateline,
                    o.uid,
                    row_number() over(partition by o.uid,o.order_status order by o.dateline) order_by
                    from 
                    ofood_dw_ods.ods_sqoop_base_jh_order_df o
                    where o.dt = '${dt}'
                ) o on o.order_by = 1 and o.order_status = 8 and from_unixtime(o.dateline,'yyyy-MM-dd') = '${dt}'
                where isInArea(b.points,o.o_lat,o.o_lng) = 1
                group by b.bd_id,b.name,b.area_id,b.area_name
                
            )
    
    
    
            insert overwrite table ofood_bi.ofood_area_bd_metrics_info partition (dt = '${dt}')
            select 
            
            
            od.bd_id as bd_id,
            od.name as username,
            od.area_name as area_name,
            0 as shop_id,
            '' as title,
            0 as total_number_of_merchants,
            0 as total_number_of_new_merchants,
            0 as trade_number_of_merchants,
            0 as have_price_number_of_merchants,
            0 as number_of_new_merchants,
            od.pay_order_num as number_of_pay_orders,
            nvl(nd.new_user_complete_num,0) as number_of_new_users,
            od.trade_price_sum as gmv,
            od.amount_price_sum as paid_in_amount,
            od.cost_price_sum as platform_subsidies,
            0 as amount_and_first_price,
            od.actual_trade_price_sum as net_turnover,
            od.order_num as total_number_of_orders,
            od.success_order_num as number_of_valid_order,
            od.before_pay_cancel_num as number_of_cancel_order_before_payment,
            od.after_pay_cancel_num as number_of_cancel_order_after_payment,
            0 as total_number_of_invitation_new_users,
            0 as number_of_invitation_new_users,
            od.area_id as area_id
            
            from 
            order_data od 
            left join new_data nd on od.bd_id = nd.bd_id
            ;
        "
        
        echo ${bd_sql}
        beeline -u "jdbc:hive2://10.52.5.190:10000/default" -n airflow -e   "${bd_sql}" 
        
        """,
    dag=dag)

insert_bd_metrics = HiveToMySqlTransfer(
    task_id='insert_bd_metrics',
    sql=""" 
        select 
        null,
        bd_id,
        dt,
        username,
        area_name,
        shop_id,
        title,
        total_number_of_merchants,
        total_number_of_new_merchants,
        trade_number_of_merchants,
        have_price_number_of_merchants,
        number_of_new_merchants,
        number_of_pay_orders,
        number_of_new_users,
        gmv,
        paid_in_amount,
        platform_subsidies,
        amount_and_first_price,
        net_turnover,
        total_number_of_order,
        number_of_valid_order,
        number_of_cancel_order_before_payment,
        number_of_cancel_order_after_payment,
        total_number_of_invitation_new_users,
        number_of_invitation_new_users,
        area_id

        from 
        ofood_bi.ofood_area_bd_metrics_info
        where dt = '{{ ds }}'


        """,
    mysql_conn_id='mysql_bi',
    mysql_table='ofood_area_bd_metrics_info',
    dag=dag)

validate_partition_data >> jh_order_validate_task >> create_bd_data
validate_partition_data >> bd_admin_users_validate_task >> create_bd_data
validate_partition_data >> bd_bd_fence_validate_task >> create_bd_data
validate_partition_data >> bd_invitation_info_validate_task >> create_bd_data

create_bd_data >> insert_bd_metrics
