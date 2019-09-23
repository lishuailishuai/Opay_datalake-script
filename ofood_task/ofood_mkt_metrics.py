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
    'start_date': datetime(2019, 9, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'ofood_mkt_metrics',
    schedule_interval="10 04 * * *",
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
                'ofood_dw_ods.ods_sqoop_bd_invitation_info_df',
            ],
        # 任务名称
        "task_name": "ofood mkt指标"
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

bd_invitation_info_validate_task = HivePartitionSensor(
    task_id="bd_invitation_info_validate_task",
    table="ods_sqoop_bd_invitation_info_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

create_mkt_metrics = HiveOperator(
    task_id='create_mkt_metrics',
    hql=''' 

        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table ofood_bi.ofood_mkt_daily_metrics_info partition(dt='{{ ds }}')
        select 
        t.mkt_id,
        t.mkt_phone,
        t.bd_id,
        t.bd_name,
        count(if(o.uid is null,null,o.uid)) as new_user_cnt,
        sum(if(p.pay_order_cnt is null,0,pay_order_cnt)) as pay_order_cnt
        from 
        (

            select 
            a.id as mkt_id,
            a.name,
            a.phone as mkt_phone,
            u.id as bd_id,
            u.name as bd_name
            from 
            (	 
                select 
                id,
                name,
                phone,
                leader_id
                from 
                ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df
                where dt = '{{ ds }}'
                and job_id = 5
            ) a 
            left join (
                select 
                id,
                name,
                phone
                from 
                ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df
                where dt = '{{ ds }}'
            ) u on a.leader_id = u.id

        ) t 
        left join (
            select 
            bd,
            uid,
            dateline
            from ofood_dw_ods.ods_sqoop_bd_invitation_info_df
            where dt = '{{ ds }}'
        ) i on t.mkt_phone = i.bd
        left join (
            select 
            o.uid
            from 
            (
                select 
                uid,
                lasttime,
                row_number() over(partition by uid,lasttime order by lasttime) as order_by
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_df
                where dt = '{{ ds }}'
                and order_status = 8 
                group by uid,lasttime
            ) o 
            where o.order_by = 1 
            and from_unixtime(o.lasttime,'yyyy-MM-dd') = '{{ ds }}'
        ) o on i.uid = o.uid
        left join (
            select 
            uid,
            from_unixtime(lasttime,'yyyy-MM-dd') as lasttime,
            count(order_id) as pay_order_cnt
            from 
            ofood_dw_ods.ods_sqoop_base_jh_order_df
            where dt = '{{ ds }}'
            and pay_status = 1
            and from_unixtime(lasttime,'yyyy-MM-dd') = '{{ ds }}'
            group by uid,from_unixtime(lasttime,'yyyy-MM-dd')
        ) p on p.uid = i.uid and p.lasttime = from_unixtime(i.dateline,'yyyy-MM-dd')
        group by t.mkt_id,t.mkt_phone,t.bd_id,t.bd_name
        ;

        ''',
    schema='ofood_bi',
    dag=dag)

insert_mkt_metrics = HiveToMySqlTransfer(
    task_id='insert_mkt_metrics',
    sql=""" 

        select 
        null,
        dt,
        mkt_id,
        mkt_phone,
        bdm_id,
        bdm_name,
        new_user_cnt,
        pay_order_cnt
        from 
        ofood_bi.ofood_mkt_daily_metrics_info
        where dt = '{{ ds }}'

        """,
    mysql_conn_id='mysql_bi',
    mysql_table='ofood_mkt_daily_metrics_info',
    dag=dag)

validate_partition_data >> jh_order_validate_task >> create_mkt_metrics
validate_partition_data >> bd_admin_users_validate_task >> create_mkt_metrics
validate_partition_data >> bd_invitation_info_validate_task >> create_mkt_metrics
create_mkt_metrics >> insert_mkt_metrics
