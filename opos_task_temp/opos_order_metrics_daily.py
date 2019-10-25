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
    'start_date': datetime(2019, 10, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'opos_order_metrics_daily',
    schedule_interval="10 04 * * *",
    default_args=args)

# 熔断阻塞流程

ods_sqoop_base_bd_shop_df_dependence_task = HivePartitionSensor(
    task_id="ods_sqoop_base_bd_shop_df_dependence_task",
    table="ods_sqoop_base_bd_shop_df",
    partition="dt='{{ds}}'",
    schema="opos_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


ods_sqoop_base_transactions_di_dependence_task = HivePartitionSensor(
    task_id="ods_sqoop_base_transactions_di_dependence_task",
    table="ods_sqoop_base_transactions_di",
    partition="dt='{{ds}}'",
    schema="opos_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_agents_df_dependence_task = HivePartitionSensor(
    task_id="ods_sqoop_base_agents_df_dependence_task",
    table="ods_sqoop_base_agents_df",
    partition="dt='{{ds}}'",
    schema="opos_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

insert_opos_order_metrics = HiveOperator(
    task_id='insert_opos_order_metrics',
    hql=''' 
        with 
        shop_dim as (
        
            select 
            s.bd_id,
            s.city_id,
            s.opay_id,
            a.agent_id
            from 
            (
                select 
                id,
                bd_id,
                opay_id,
                city_id
                from 
                opos_dw_ods.ods_sqoop_base_bd_shop_df
                where dt = '{dt}'
            ) s 
            join (
                select 
                id as agent_id,
                agent_id as opay_id
                from 
                opos_dw_ods.ods_sqoop_base_agents_df
                where dt = '{dt}'
            ) a on s.opay_id = a.opay_id
            
        ),
        
        order_metrcis as (
            select 
            t.dt,
            s.bd_id,
            s.city_id,
            count(if(t.status_code = '00' ,t.id,null)) as complete_order_cnt,
            count(distinct(t.agent_id)) as accept_agent_cnt,
            count(distinct(t.card_hash)) as place_user_cnt,--有单用户数
            count(distinct(if(t.status_code = '00' , t.agent_id,null))) as complete_agent_cnt,
            sum(t.amount) as gmv
            
            from 
            
            (   
                select
                dt,
                id,
                agent_id,
                status_code,
                amount,
                card_hash
                from 
                opos_dw_ods.ods_sqoop_base_transactions_di
                where dt = '{dt}'
                and substr(created,1,10) = '{dt}'
            ) t 
            join shop_dim s on  t.agent_id = s.agent_id
            group by t.dt,s.bd_id,s.city_id
        ),
        
        
        agent_metrics as (
            select 
            a.dt,
            s.bd_id,
            s.city_id,
            count(distinct(a.agent_id)) as all_agent_cnt,
            count(distinct(if(a.status = '00', a.agent_id,null))) as pos_agent_cnt,
            count(distinct(if(substr(a.created,1,10) = '{dt}',a.agent_id,null))) as new_agent_cnt
            from 
            (
                select 
                dt,
                id as agent_id, 
                status,
                created
                from 
                opos_dw_ods.ods_sqoop_base_agents_df
                where dt = '{dt}'
            ) a 
            join shop_dim s on  a.agent_id = s.agent_id
            group by a.dt,s.bd_id,s.city_id
        )
        
        
        insert overwrite table opos_temp.opos_metrcis_report partition (dt = '{dt}')
        select 
        am.bd_id,
        am.city_id,
        am.all_agent_cnt,
        am.pos_agent_cnt,
        am.new_agent_cnt,
        nvl(om.complete_order_cnt,0),
        nvl(om.accept_agent_cnt,0),
        nvl(om.place_user_cnt,0),
        nvl(om.complete_agent_cnt,0),
        nvl(om.gmv,0)
        from 
        agent_metrics am 
        left join 
        order_metrcis om on am.dt = om.dt and am.bd_id = om.bd_id and am.city_id = om.city_id
        ;

        '''.format(
        dt='{{ ds }}'
    ),
    schema='opos_temp',
    dag=dag)

insert_opos_active_user_detail_metrics = HiveOperator(
    task_id='insert_opos_active_user_detail_metrics',
    hql=''' 
    
        with 
        shop_dim as (
        
            select 
            s.bd_id,
            s.city_id,
            s.opay_id,
            a.agent_id
            from 
            (
                select 
                id,
                bd_id,
                opay_id,
                city_id
                
                from 
                opos_dw_ods.ods_sqoop_base_bd_shop_df
                where dt = '{dt}'
            ) s 
            join (
                select 
                id as agent_id,
                agent_id as opay_id
                from 
                opos_dw_ods.ods_sqoop_base_agents_df
                where dt = '{dt}'
            ) a on s.opay_id = a.opay_id
            
        )
        
        insert overwrite table opos_temp.opos_active_user_detail_daily partition (dt = '{dt}')
        select 
        
        sd.bd_id,
        sd.city_id,
        t.card_hash
        from 
        (   
            select 
            agent_id,
            card_hash
            from 
            opos_dw_ods.ods_sqoop_base_transactions_di
            where dt = '{dt}'
            and substr(created,1,10) = '{dt}'
            and status_code = '00'
        ) t 
        join 
        shop_dim sd on t.agent_id = sd.agent_id
        group by sd.bd_id,sd.city_id,t.card_hash
        
        ;
        '''.format(
        dt='{{ ds }}'
    ),
    schema='opos_temp',
    dag=dag)

insert_opos_active_user_metrics = HiveOperator(
    task_id='insert_opos_active_user_metrics',
    hql='''
        with user_base as (
            select 
            bd_id,
            city_id,
            card_hash,
            sum(if(dt = '{dt}',1,0)) as is_current_day,
            sum(if(dt = '{before_1_day}',1,0)) as is_before_1_day,
            sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day,
            sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day,
            sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
            from 
            opos_temp.opos_active_user_detail_daily
            where dt in ('{dt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
            group by bd_id,
            city_id,
            card_hash
        ),
        
        current_user as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(card_hash) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0
            group by 
            bd_id,
            city_id
        ),
        
        
        day_1_remain as (
            
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(card_hash) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0 and is_before_1_day > 0
            group by 
            bd_id,
            city_id
        ),
        
        day_7_remain as (
            
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(card_hash) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0 and is_before_7_day > 0
            group by 
            bd_id,
            city_id
        ),
        
        day_15_remain as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(card_hash) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0 and is_before_15_day > 0
            group by 
            bd_id,
            city_id
        ),
        
        day_30_remain as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(card_hash) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0 and is_before_30_day > 0
            group by 
            bd_id,
            city_id
        )
        
        insert overwrite table opos_temp.opos_active_user_daily partition(dt = '{dt}')
        select 
        cu.bd_id,
        cu.city_id,
        cu.user_active_cnt,
        nvl(dr1.user_active_cnt,0),
        nvl(dr7.user_active_cnt,0),
        nvl(dr15.user_active_cnt,0),
        nvl(dr30.user_active_cnt,0)
        from 
        current_user cu
        left join day_1_remain dr1 on cu.dt = dr1.dt and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
        left join day_7_remain dr7 on cu.dt = dr7.dt and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
        left join day_15_remain dr15 on cu.dt = dr15.dt and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
        left join day_30_remain dr30 on cu.dt = dr30.dt and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
        ;

        '''.format(
        dt='{{ ds }}',
        before_1_day='{{ macros.ds_add(ds, -1) }}',
        before_7_day='{{ macros.ds_add(ds, -7) }}',
        before_15_day='{{ macros.ds_add(ds, -15) }}',
        before_30_day='{{ macros.ds_add(ds, -30) }}',
    ),
    schema='opos_temp',
    dag=dag)

insert_crm_metrics = HiveToMySqlTransfer(
    task_id='insert_crm_metrics',
    sql=""" 
        select 
        null,
        a.dt,
        a.bd_id,
        a.city_id,
        a.all_agent_cnt,
        a.pos_agent_cnt,
        a.new_agent_cnt,
        a.complete_order_cnt,
        a.accept_agent_cnt,
        a.place_user_cnt,
        a.complete_agent_cnt,
        a.gmv,
        nvl(b.current_day_user_active_cnt,0),
        nvl(b.before_1_day_user_active_cnt,0),
        nvl(b.before_7_day_user_active_cnt,0),
        nvl(b.before_15_day_user_active_cnt,0),
        nvl(b.before_30_day_user_active_cnt,0)
        
        from 
        opos_temp.opos_metrcis_report a 
        left join opos_temp.opos_active_user_daily b on a.dt = b.dt and a.bd_id = b.bd_id and a.city_id = b.city_id       
        where a.dt = '{{ ds }}'

        """,
    mysql_conn_id='mysql_dw',
    mysql_table='opos_metrics_daily',
    dag=dag)


ods_sqoop_base_agents_df_dependence_task >> insert_opos_order_metrics
ods_sqoop_base_transactions_di_dependence_task >> insert_opos_order_metrics
ods_sqoop_base_bd_shop_df_dependence_task >> insert_opos_order_metrics
ods_sqoop_base_transactions_di_dependence_task >> insert_opos_active_user_detail_metrics
ods_sqoop_base_bd_shop_df_dependence_task >> insert_opos_active_user_detail_metrics

insert_opos_active_user_detail_metrics >> insert_opos_active_user_metrics

insert_opos_order_metrics >> insert_crm_metrics
insert_opos_active_user_metrics >> insert_crm_metrics