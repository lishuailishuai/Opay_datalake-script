# -*- coding: utf-8 -*-
"""
调度算法效果监控指标新版2019-08-02
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging

args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 10, 21),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_daily_report_d', # check
    schedule_interval="30 04 * * *",
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
# ods_sqoop_base_user_df
dependence_ods_sqoop_base_user_df = HivePartitionSensor(
    task_id="ods_sqoop_base_user_df",
    table="ods_sqoop_base_user_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)

# ods_sqoop_base_big_order_df
dependence_ods_sqoop_base_big_order_df = HivePartitionSensor(
    task_id="dependence_ods_sqoop_base_big_order_df",
    table="ods_sqoop_base_big_order_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)
# ods_sqoop_base_user_transfer_user_record_df
dependence_ods_sqoop_base_user_transfer_user_record_df = HivePartitionSensor(
    task_id="dependence_ods_sqoop_base_user_transfer_user_record_df",
    table="ods_sqoop_base_user_transfer_user_record_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)
# ods_sqoop_base_merchant_transfer_user_record_df
dependence_ods_sqoop_base_merchant_transfer_user_record_df = HivePartitionSensor(
    task_id="dependence_ods_sqoop_base_merchant_transfer_user_record_df",
    table="ods_sqoop_base_merchant_transfer_user_record_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)
# ods_sqoop_base_airtime_topup_record_df
dependence_ods_sqoop_base_airtime_topup_record_df = HivePartitionSensor(
    task_id="dependence_ods_sqoop_base_airtime_topup_record_df",
    table="ods_sqoop_base_airtime_topup_record_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)
# ods_sqoop_base_user_topup_record_df
dependence_ods_sqoop_base_user_topup_record_df = HivePartitionSensor(
    task_id="dependence_ods_sqoop_base_user_topup_record_df",
    table="ods_sqoop_base_user_topup_record_df", # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)


"""
##-----end-------##
"""

hive_table = 'opay.app_opay_daily_report_d' # check table_name


insert_result_to_impala = HiveOperator(
    task_id='insert_result_to_impala',
    hql="""
        set mapred.job.queue.name=root.users.airflow;
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nostrict;
        with
        --总交易 
        big_order_data as (
            select 
                '{pt}' `state_date`,
                sum(if(service_type = 'AATransfer', nvl(amount,0), 0)) `aa_transfer_amt`, 
                sum(if(service_type = 'AATransfer', 1, 0)) `aa_transfer_cntt`, 
            
                sum(if(service_type ='ACTransfer', nvl(amount,0), 0)) `ac_transfer_amt`, 
                sum(if(service_type = 'ACTransfer', 1, 0)) `ac_transfer_cnt`, 
            
                sum(if(service_type = 'Airtime', nvl(amount,0), 0)) `airtime_amt`, 
                sum(if(service_type = 'Airtime', 1, 0))`airtime_cnt`, 
            
                sum(if(service_type = 'Betting', nvl(amount,0), 0)) `betting_amt`, 
                sum(if(service_type = 'Betting', 1, 0))`betting_cnt`, 
            
                sum(if(service_type = 'easycash', nvl(amount,0), 0)) `easycash_amt`,
                sum(if(service_type = 'easycash', 1, 0)) `easycash_cnt`,
            
                sum(if(service_type = 'Electricity', nvl(amount,0), 0)) `electricity_amt`,
                sum(if(service_type = 'Electricity', 1, 0)) `electricity_cnt`, 
            
                sum(if(service_type = 'MAcquiring', nvl(amount,0), 0)) `m_acquiring_amt`,
                sum(if(service_type = 'MAcquiring', 1, 0)) `m_acquiring_cnt`, 
            
                sum(if(service_type = 'MobileData', nvl(amount,0), 0)) `mobile_data_amt`,
                sum(if(service_type = 'MobileData', 1, 0)) `mobile_data_cnt`, 
            
                sum(if(service_type = 'pos', nvl(amount,0), 0)) `pos_amt`,
                sum(if(service_type = 'pos', 1, 0)) `pos_cnt`, 
            
                sum(if(service_type = 'receivemoney', nvl(amount,0), 0)) `receivemoney_amt`,
                sum(if(service_type = 'receivemoney', 1, 0)) `receivemoney_cnt`,
            
                sum(if(service_type = 'TopupWithCard', nvl(amount,0), 0)) `topup_with_card_amt`,
                sum(if(service_type = 'TopupWithCard', 1, 0)) `topup_with_card_cnt`, 
            
                sum(if(service_type = 'TV', nvl(amount,0), 0)) `tv_amt`,
                sum(if(service_type = 'TV', 1, 0)) `tv_cnt`,
            
                sum(amount) `st_summary_amt`, 
                count(order_no) `st_summary_cnt` 
            from opay_dw_ods.ods_sqoop_base_big_order_df
            where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
        ),
        --C端用户交易
        p2p_data as (
            select 
                '{pt}' state_date,
                sum(if(uu.`role`='agent' and ur.`role`='customer', ut.amount, 0)) `cash_in_amt`,
                sum(if(uu.`role`='agent' and ur.`role`='customer', 1, 0)) `cash_in_cnt`,
                sum(if(uu.`role`='customer' and ur.`role`='customer', ut.amount, 0)) `p2p_transfer_amt`,
                sum(if(uu.`role`='customer' and ur.`role`='customer', 1, 0)) `p2p_transfer_cnt`,
                sum(if(uu.`role`='customer' and ur.`role`='agent', ut.amount, 0)) `cash_out_amt`,
                sum(if(uu.`role`='customer' and ur.`role`='agent', 1, 0)) `cash_out_cnt`
            from
            (
                select 
                    user_id, recipient_id, nvl(amount,0) `amount`
                from opay_dw_ods.ods_sqoop_base_user_transfer_user_record_df 
                where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and transfer_status='TRANSFER_S'
            ) ut
            join
            (
                select 
                    user_id, `role` 
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}'
            ) uu on ut.user_id=uu.user_id
            join
            (
                select 
                    user_id, `role` 
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}'
            ) ur on ut.recipient_id = ur.user_id
        ),
        --C2B Transfer
        c2b_data as (
            select 
                '{pt}' state_date,
                sum(amount) `c2b_transfer_amt`, count(*) `c2b_transfer_cnt`
            from
            (
                select 
                    user_id, recipient_id, nvl(amount, 0) `amount`
                from opay_dw_ods.ods_sqoop_base_user_transfer_user_record_df 
                where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and transfer_status='TRANSFER_S' and recipient_type='MERCHANT'
            ) ut
            join
            (
                select 
                    user_id
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}' and `role`='customer'
            ) uu on ut.user_id=uu.user_id
        ),
        --B2C Transfer
        b2c_data as (
            select 
                '{pt}' state_date, sum(amount) `b2c_transfer_amt`, count(*) `b2c_transfer_cnt`
            from
            (
                select 
                    recipient_id, nvl(amount, 0) `amount`
                from opay_dw_ods.ods_sqoop_base_merchant_transfer_user_record_df 
                where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
            ) ut
            join
            (
                select 
                    user_id
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}' and `role`='customer'
            ) ur on ut.recipient_id = ur.user_id
        ),
        --用户交易
        customer_big_order_data as (
            select 
                '{pt}' state_date,
                sum(if(t1.service_type ='ACTransfer', t1.amount, 0)) `ac_transfer_customer_amt`, 
                sum(if(t1.service_type = 'ACTransfer', 1, 0)) `ac_transfer_customer_cnt`, 
            
                sum(if(t1.service_type = 'Airtime', t1.amount, 0)) `airtime_customer_amt`, 
                sum(if(t1.service_type = 'Airtime', 1, 0))`airtime_customer_cnt`, 
            
                sum(if(t1.service_type = 'Betting', t1.amount, 0)) `betting_customer_amt`, 
                sum(if(t1.service_type = 'Betting', 1, 0))`betting_customer_cnt`, 
                
                sum(if(t1.service_type = 'Electricity', t1.amount, 0)) `electricity_customer_amt`, 
                sum(if(t1.service_type = 'Electricity', 1, 0))`electricity_customer_cnt`, 
            
                sum(if(t1.service_type = 'MobileData', t1.amount, 0)) `mobile_data_customer_amt`, 
                sum(if(t1.service_type = 'MobileData', 1, 0))`mobile_data_customer_cnt`, 
            
                sum(if(t1.service_type = 'receivemoney', t1.amount, 0)) `receivemoney_customer_amt`, 
                sum(if(t1.service_type = 'receivemoney', 1, 0))`receivemoney_customer_cnt`, 
            
            
                sum(if(t1.service_type = 'TopupWithCard', t1.amount, 0)) `topup_with_card_customer_amt`, 
                sum(if(t1.service_type = 'TopupWithCard', 1, 0))`topup_with_card_customer_cnt`, 
            
                sum(if(t1.service_type = 'TV', t1.amount, 0)) `tv_customer_amt`,
                sum(if(t1.service_type = 'TV', 1, 0)) `tv_customer_cnt`, 
            
                sum(amount) `st_summary_customer_amt`, 
                count(order_no) `st_summary_customer_cnt` 
            from
            (
                select 
                    user_id, order_no, service_type, nvl(amount, 0) `amount`
                from opay_dw_ods.ods_sqoop_base_big_order_df
                where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
            ) t1
            join 
            (
                select 
                    user_id
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}'  and lower(role)='customer'
            ) t2
            on t2.user_id=t1.user_id
        ),
        airtime_topup_data as (
            select 
                '{pt}' state_date,
                sum(if(telecom_perator = 'AIR', nvl(amount,0), 0)) `air_amt`, 
                sum(if(telecom_perator = 'AIR', 1, 0)) `air_cnt`, 
            
                sum(if(telecom_perator = 'ETI', nvl(amount,0), 0)) `eti_amt`, 
                sum(if(telecom_perator = 'ETI', 1, 0)) `eti_cnt`, 
                
                sum(if(telecom_perator = 'GLO', nvl(amount,0), 0)) `glo_amt`, 
                sum(if(telecom_perator = 'GLO', 1, 0)) `glo_cnt`, 
            
                sum(if(telecom_perator = 'MTN', nvl(amount,0), 0)) `mtn_amt`, 
                sum(if(telecom_perator = 'MTN', 1, 0)) `mtn_cnt`	
            from opay_dw_ods.ods_sqoop_base_airtime_topup_record_df
            where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
        ),
        betting_topup_data as (
            SELECT 
                '{pt}' state_date,
                sum(if(betting_provider = 'BET9JA', nvl(amount,0), 0)) `bet9ja_amt`, 
                sum(if(betting_provider = 'BET9JA', 1, 0)) `bet9ja_cnt`, 
            
                sum(if(betting_provider = 'SUPABET', nvl(amount,0), 0)) `supabet_amt`, 
                sum(if(betting_provider = 'SUPABET', 1, 0)) `supabet_cnt`
            from opay_dw_ods.ods_sqoop_base_betting_topup_record_df 
            where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
        ),
        agent_ordered_data as (
            select
                '{pt}' state_date,
                count(distinct t2.user_id) `agent_ordered_cnt`
            from
            (
                select
                    user_id, order_no, nvl(amount,0) `amount`
                from opay_dw_ods.ods_sqoop_base_big_order_df 
                where dt = '{pt}'
                    and date_format(create_time, 'yyyy-MM-dd')
                    between date_sub('{pt}', 30) and '{pt}'
                    and lower(order_status)='success'
            ) t1 inner join (
                select 
                    user_id
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = '{pt}'  and lower(role)='agent'
            ) t2
            on t2.user_id=t1.user_id
        ),
        agent_cnt_data as (
            select '{pt}' state_date, count(user_id) `agent_cnt`
            from opay_dw_ods.ods_sqoop_base_user_df
            where dt = '{pt}'  and lower(role)='agent'
        ),
        bind_card_data as (
            SELECT 
                '{pt}' state_date,
                sum(if(out_channel_id = 'Paystack', nvl(amount,0), 0)) `paystack_amt`, 
                sum(if(out_channel_id = 'Paystack', 1, 0)) `paystack_cnt`, 
            
                sum(if(out_channel_id = 'InterSwitch', nvl(amount,0), 0)) `inter_switch_amt`, 
                sum(if(out_channel_id = 'InterSwitch', 1, 0)) `inter_switch_cnt`,
            
                sum(if(out_channel_id = 'GTGIPS', nvl(amount,0), 0)) `gtgips_amt`, 
                sum(if(out_channel_id = 'GTGIPS', 1, 0)) `gtgips_cnt`,
            
                sum(if(out_channel_id = 'Flutterwave', nvl(amount,0), 0)) `flutterwave_amt`, 
                sum(if(out_channel_id = 'Flutterwave', 1, 0)) `flutterwave_cnt`
            from opay_dw_ods.ods_sqoop_base_user_topup_record_df
            where dt = '{pt}' and date_format(create_time, 'yyyy-MM-dd')='{pt}' and order_status='SUCCESS'
        )
        insert overwrite table {table_name} PARTITION (dt='{pt}')
        select 
            t1.*,
            
            t2.cash_in_amt, t2.cash_in_cnt, 
            t2.p2p_transfer_amt, t2.p2p_transfer_cnt,
            t2.cash_out_amt, t2.cash_out_cnt,
            
            t3.c2b_transfer_amt, t3.c2b_transfer_cnt,
            
            t4.b2c_transfer_amt, t4.b2c_transfer_cnt,
            
            t5.ac_transfer_customer_amt, t5.ac_transfer_customer_cnt,
            t5.airtime_customer_amt, t5.airtime_customer_cnt,
            t5.betting_customer_amt, t5.betting_customer_cnt,
            t5.electricity_customer_amt, t5.electricity_customer_cnt,
            t5.mobile_data_customer_amt, t5.mobile_data_customer_cnt,
            t5.receivemoney_customer_amt, t5.receivemoney_customer_cnt,
            t5.topup_with_card_customer_amt, t5.topup_with_card_customer_cnt,
            t5.tv_customer_amt, t5.tv_customer_cnt,
            t5.st_summary_customer_amt, t5.st_summary_customer_cnt,
            
            t6.air_amt, t6.air_cnt,
            t6.eti_amt, t6.eti_cnt,
            t6.glo_amt, t6.glo_cnt,
            t6.mtn_amt, t6.mtn_cnt,
            
            t7.bet9ja_amt, t7.bet9ja_cnt,
            t7.supabet_amt, t7.supabet_cnt,
            
            t8.agent_ordered_cnt,
            
            t9.agent_cnt,
            
            t10.paystack_amt, paystack_cnt,
            t10.inter_switch_amt, inter_switch_cnt,
            t10.gtgips_amt, t10.gtgips_cnt,
            t10.flutterwave_amt, t10.flutterwave_cnt
        from big_order_data t1
        join p2p_data t2 on t1.state_date=t2.state_date
        join c2b_data t3 on t1.state_date=t3.state_date
        join b2c_data t4 on t1.state_date=t4.state_date
        join customer_big_order_data t5 on t1.state_date=t5.state_date
        join airtime_topup_data t6 on t1.state_date=t6.state_date
        join betting_topup_data t7 on t1.state_date=t7.state_date
        join agent_ordered_data t8 on t1.state_date=t8.state_date
        join agent_cnt_data t9 on t1.state_date=t9.state_date
        join bind_card_data t10 on t1.state_date=t10.state_date
    """.format(pt='{{ ds }}', table_name=hive_table),
    schema='opay',
    priority_weight=50,
    dag=dag
)

dependence_ods_sqoop_base_user_df >> dependence_ods_sqoop_base_big_order_df >> dependence_ods_sqoop_base_user_transfer_user_record_df >> dependence_ods_sqoop_base_merchant_transfer_user_record_df >> dependence_ods_sqoop_base_airtime_topup_record_df >> dependence_ods_sqoop_base_user_topup_record_df >> insert_result_to_impala
