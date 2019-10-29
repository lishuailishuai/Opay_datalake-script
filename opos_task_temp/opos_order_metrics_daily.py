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
    'start_date': datetime(2019, 10, 28),
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


ods_sqoop_base_pre_opos_payment_order_di_dependence_task = HivePartitionSensor(
    task_id="ods_sqoop_base_pre_opos_payment_order_di_dependence_task",
    table="ods_sqoop_base_pre_opos_payment_order_di",
    partition="dt='{{ds}}'",
    schema="opos_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



insert_opos_order_metrics = HiveOperator(
    task_id='insert_opos_order_metrics',
    hql=''' 
        
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        with merchant_data as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(id) as merchant_cnt,
            0 as pos_merchant_cnt,
            count(if(substr(created_at,1,10) = '{dt}',id,null)) as new_merchant_cnt,
            0 as new_pos_merchant_cnt
            from 
            opos_dw_ods.ods_sqoop_base_bd_shop_df
            where dt = '{dt}'
            and substr(created_at,1,10) <= '{dt}'
            group by bd_id,city_id
        ),
        
        order_data as (
            select 
            '{dt}' as dt,
            s.bd_id,
            s.city_id,
            count(if(p.order_type = 'pos' and p.trade_status = 'SUCCESS',p.order_id,null)) as pos_complete_order_cnt,
            count(if(p.order_type = 'qrcode' and p.trade_status = 'SUCCESS',p.order_id,null)) as qr_complete_order_cnt,
            count(if(p.trade_status = 'SUCCESS',p.order_id,null)) as complete_order_cnt,
            sum(if(p.trade_status = 'SUCCESS',p.org_payment_amount,0)) as gmv,
            sum(if(p.trade_status = 'SUCCESS',p.pay_amount,0)) as actual_amount
            
            from 
            (
                select 
                receipt_id,
                order_id,
                order_type,
                trade_status,
                org_payment_amount,
                pay_amount
                from 
                opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di
                where dt = '{dt}'
            ) p 
            join (
                select 
                bd_id,
                city_id,
                opay_id
                from 
                opos_dw_ods.ods_sqoop_base_bd_shop_df
                where dt = '{dt}'
                and substr(created_at,1,10) <= '{dt}'
            ) s on p.receipt_id = s.opay_id
            group by s.bd_id,s.city_id
        ) 
        
        insert overwrite table opos_temp.opos_metrcis_report partition (country_code,dt)
        select 
        md.bd_id,
        md.city_id,
        md.merchant_cnt,
        md.pos_merchant_cnt,
        md.new_merchant_cnt,
        md.new_pos_merchant_cnt,
        nvl(od.pos_complete_order_cnt,0),
        nvl(od.qr_complete_order_cnt,0),
        nvl(od.complete_order_cnt,0),
        nvl(od.gmv,0),
        nvl(od.actual_amount,0),
        'nal' as country_code,
        md.dt as dt
        
        from 
        merchant_data md 
        left join 
        order_data od on md.dt = od.dt and md.bd_id = od.bd_id and md.city_id = od.city_id
        ;

        '''.format(
        dt='{{ ds }}'
    ),
    schema='opos_temp',
    dag=dag)

insert_opos_active_user_detail_metrics = HiveOperator(
    task_id='insert_opos_active_user_detail_metrics',
    hql=''' 
    
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
    
        insert overwrite table opos_temp.opos_active_user_detail_daily partition (country_code,dt)
        select 
        s.bd_id,
        s.city_id,
        p.sender_id,
        p.receipt_id,
        p.order_type,
        p.trade_status,
        'nal' as country_code,
        '{dt}' as dt
        
        from 
        (
            select 
            sender_id,
            order_type,
            receipt_id,
            trade_status
            from 
            opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di
            where dt = '{dt}'
        ) p 
        join (
            select 
            bd_id,
            city_id,
            opay_id
            from 
            opos_dw_ods.ods_sqoop_base_bd_shop_df
            where dt = '{dt}'
            and substr(created_at,1,10) <= '{dt}'
        ) s on p.receipt_id = s.opay_id
        
        ;
        '''.format(
        dt='{{ ds }}'
    ),
    schema='opos_temp',
    dag=dag)

insert_opos_active_user_metrics = HiveOperator(
    task_id='insert_opos_active_user_metrics',
    hql='''
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        with active_base as (
            select 
            * 
            from 
            opos_temp.opos_active_user_detail_daily
            where country_code = 'nal' and dt in ('{dt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
        ),
    
        user_base as (
            select 
            bd_id,
            city_id,
            sender_id,
            order_type,
            sum(if(dt = '{dt}',1,0)) as is_current_day,
            sum(if(dt = '{before_1_day}',1,0)) as is_before_1_day,
            sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day,
            sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day,
            sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
            from 
            active_base 
            where trade_status = 'SUCCESS'
            group by bd_id,
            city_id,
            sender_id,
            order_type
        ),
        
        current_user as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt,
            count(if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
            
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
            count(distinct sender_id) as user_active_cnt
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
            count(distinct sender_id) as user_active_cnt
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
            count(distinct sender_id) as user_active_cnt
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
            count(distinct sender_id) as user_active_cnt
            from 
            user_base 
            where is_current_day > 0 and is_before_30_day > 0
            group by 
            bd_id,
            city_id
        ),
        
        order_merchant_data as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(distinct(receipt_id)) as order_merchant_cnt,
            count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
            from 
            active_base
            where dt = '{dt}' and trade_status = 'SUCCESS'
            group by bd_id,
            city_id
        ),
        
        time_dim as (
            select 
            *
            from 
            public_dw_dim.dim_date
            where dt = '{dt}'
        ),
        
        
        week_data as (
            select 
            '{dt}' as dt,
            u.bd_id,
            u.city_id,
            t.monday_of_year,
            count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
            count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt
            
            
            from 
            (
                select 
                *
                from 
                opos_temp.opos_active_user_detail_daily 
                where country_code = 'nal' 
                and dt between '{dt}' and '{before_30_day}'
                and trade_status = 'SUCCESS' 
            ) u 
            join public_dw_dim.dim_date d on u.dt = d.dt
            join time_dim t on d.monday_of_year = t.monday_of_year
            group by t.monday_of_year,u.bd_id,u.city_id
        ),
        
        month_data as (
            select 
            '{dt}' as dt,
            u.bd_id,
            u.city_id,
            t.month,
            count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
            count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt
            
            from 
            (
                select 
                *
                from 
                opos_temp.opos_active_user_detail_daily 
                where country_code = 'nal' 
                and dt between '{dt}' and '{before_30_day}'
                and trade_status = 'SUCCESS' 
            ) u 
            join public_dw_dim.dim_date d on u.dt = d.dt
            join time_dim t on d.month = t.month
            group by t.month,u.bd_id,u.city_id
        ),
        
        have_order_user as (
            select 
            '{dt}' as dt,
            bd_id,
            city_id,
            count(distinct(sender_id)) as have_order_user_cnt
            from 
            active_base
            where dt = '{dt}'
            group by bd_id,
            city_id
        )
        
        
        insert overwrite table opos_temp.opos_active_user_daily partition(country_code,dt)
        select 
        cu.bd_id,
        cu.city_id,
        cu.pos_user_active_cnt,
        cu.qr_user_active_cnt,
        nvl(dr1.user_active_cnt,0),
        nvl(dr7.user_active_cnt,0),
        nvl(dr15.user_active_cnt,0),
        nvl(dr30.user_active_cnt,0),
        nvl(omd.order_merchant_cnt,0),
        nvl(omd.pos_order_merchant_cnt,0),
        nvl(wd.pos_user_active_cnt,0),
        nvl(wd.qr_user_active_cnt,0),
        nvl(md.pos_user_active_cnt,0),
        nvl(md.qr_user_active_cnt,0),
        nvl(ou.have_order_user_cnt,0),
        
        'nal' as country_code,
        '{dt}' as dt
        
        from 
        current_user cu
        left join day_1_remain dr1 on cu.dt = dr1.dt and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
        left join day_7_remain dr7 on cu.dt = dr7.dt and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
        left join day_15_remain dr15 on cu.dt = dr15.dt and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
        left join day_30_remain dr30 on cu.dt = dr30.dt and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
        left join order_merchant_data omd on cu.dt = omd.dt and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
        left join week_data wd on cu.dt = wd.dt and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
        left join month_data md on cu.dt = md.dt and cu.bd_id = md.bd_id and cu.city_id = md.city_id
        left join have_order_user ou on cu.dt = ou.dt and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
        
        
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
        a.bd_id , 
        a.city_id , 
        a.merchant_cnt , 
        a.pos_merchant_cnt , 
        a.new_merchant_cnt , 
        a.new_pos_merchant_cnt , 
        a.pos_complete_order_cnt , 
        a.qr_complete_order_cnt , 
        a.complete_order_cnt , 
        a.gmv ,
        a.actual_amount ,
        nvl(b.pos_user_active_cnt,0),
        nvl(b.qr_user_active_cnt,0),
        nvl(b.before_1_day_user_active_cnt,0),
        nvl(b.before_7_day_user_active_cnt,0),
        nvl(b.before_15_day_user_active_cnt,0),
        nvl(b.before_30_day_user_active_cnt,0),
        nvl(b.order_merchant_cnt,0),
        nvl(b.pos_order_merchant_cnt,0),
        nvl(b.week_pos_user_active_cnt,0),
        nvl(b.week_qr_user_active_cnt,0),
        nvl(b.month_pos_user_active_cnt,0),
        nvl(b.month_qr_user_active_cnt,0),
        nvl(b.have_order_user_cnt,0)
        
        from 
        opos_temp.opos_metrcis_report a 
        left join opos_temp.opos_active_user_daily b on  a.country_code = b.country_code and a.dt = b.dt and a.bd_id = b.bd_id and a.city_id = b.city_id       
        where a.country_code = 'nal' and  a.dt = '{{ ds }}'

        """,
    mysql_conn_id='mysql_dw',
    mysql_table='opos_metrics_daily',
    dag=dag)


ods_sqoop_base_pre_opos_payment_order_di_dependence_task >> insert_opos_order_metrics
ods_sqoop_base_bd_shop_df_dependence_task >> insert_opos_order_metrics
ods_sqoop_base_pre_opos_payment_order_di_dependence_task >> insert_opos_active_user_detail_metrics
ods_sqoop_base_bd_shop_df_dependence_task >> insert_opos_active_user_detail_metrics

insert_opos_active_user_detail_metrics >> insert_opos_active_user_metrics

insert_opos_order_metrics >> insert_crm_metrics
insert_opos_active_user_metrics >> insert_crm_metrics