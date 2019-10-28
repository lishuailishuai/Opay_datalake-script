import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_cbn_report_m',
                  schedule_interval="0 3 2 * *",
                  default_args=args)

##----依赖数据源---##
# ods_sqoop_base_user_df
dependence_ods_sqoop_base_user_df = HivePartitionSensor(
    task_id="ods_sqoop_base_user_df",
    table="ods_sqoop_base_user_df",  # 表名一至
    partition="dt='{{ yesterday_ds }}'",
    schema="opay_dw_ods",
    poke_interval=60,
    dag=dag
)
##-----end------#

##------declare variables ------##
table_name = 'opay.app_opay_cbn_report_m'
hdfs_path = 'ufile://opay-datalake/opay/opay/' + table_name
##------declare variables end ------##


##---- hive operator ---##
app_opay_cbn_report_m_task = HiveOperator(
    task_id='app_opay_cbn_report_m_task',
    hql='''
    set hive.exec.parallel=true;

    set hive.exec.dynamic.partition.mode=nonstrict;
    with
    --1 本月累计注册用户数
    new_user_data as (
        select
            '{pt}' state_month,
            count(distinct user_id) new_user_cnt
        from opay_dw_ods.ods_sqoop_base_user_df
        where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM') and role='customer'
    ),
    --2 近6月成功交易用户数
    recently_trade_data as(
        select
            '{pt}' state_month, count(distinct t1.user_id) recent_trade_user_cnt
        from
        (
            select
                user_id 
            from opay_dw_ods.ods_sqoop_base_big_order_df 
            where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd')
                and create_time between date_format(add_months('{pt}', -5), 'yyyy-MM-01') and last_day('{pt}')
                and lower(order_status)='success' 
        ) t1 
        inner join 
        (
            select 
                user_id
            from opay_dw_ods.ods_sqoop_base_user_df
            where dt=date_format(add_months('{pt}', 1), 'yyyy-MM-dd') and lower(role)='customer'
        ) t2
        on t2.user_id=t1.user_id
    ),
    --3 本月KYC1、2、3总用户数 可能会存在重复计算的可能性
    kyc_user_data as (
        select  
            '{pt}' state_month,
            count(distinct if (kyc_level = 1, user_id, 0)) kyc1_user_cnt,
            count(distinct if (kyc_level = 2, user_id, 0)) kyc2_user_cnt, 
            count(distinct if (kyc_level = 3, user_id, 0)) kyc3_user_cnt

        from opay_dw_ods.ods_sqoop_base_user_df
        where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(kyc_update_time, 'yyyy-MM')=date_format('{pt}', 'yyyy-MM') and `role`='customer'
    ),
    -- 5、本月总成功交易额-AAtransfer 、ACtransfer 、Airtime、TV、MAcquiring
    big_order_trade_data as (
        select     
            '{pt}' state_month,
            sum(if(service_type = 'AATransfer', nvl(amount,0), 0)) `aa_transfer_amt`, 
            sum(if(service_type = 'AATransfer', 1, 0)) `aa_transfer_cnt`, 

            sum(if(service_type ='ACTransfer', nvl(amount,0), 0)) `ac_transfer_amt`, 
            sum(if(service_type = 'ACTransfer', 1, 0)) `ac_transfer_cnt`, 

            sum(if(service_type = 'Airtime', nvl(amount,0), 0)) `airtime_amt`, 
            sum(if(service_type = 'Airtime', 1, 0))`airtime_cnt`, 

            sum(if(service_type = 'MAcquiring', nvl(amount,0), 0)) `m_acquiring_amt`,
            sum(if(service_type = 'MAcquiring', 1, 0)) `m_acquiring_cnt`, 

            sum(if(service_type = 'TV', nvl(amount,0), 0)) `tv_amt`,
            sum(if(service_type = 'TV', 1, 0)) `tv_cnt`,

            sum(amount) `st_summary_amt`, 
            count(order_no) `st_summary_cnt` 
        from opay_dw_ods.ods_sqoop_base_big_order_df
        where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM')
         and order_status='SUCCESS'
    ),
    -- 本月代理充值钱包金额 --本月代理充值钱包次数
    agent_topup_data as(
        select 
            '{pt}' state_month, sum(detl.amt) agent_topup_amt, sum(detl.cnt) agent_topup_cnt
        from (
            select
                t1.user_id,
                count(t1.order_no) cnt,
                sum(t1.amount) amt
            from 
            (
                select 
                    user_id, order_no,amount
                from opay_dw_ods.ods_sqoop_base_user_topup_record_df
                where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM')
                    and lower(order_status)='success'
            ) t1 
            inner join 
            (
                select 
                    user_id
                from opay_dw_ods.ods_sqoop_base_user_df
                where dt = date_format(last_day('{pt}'), 'yyyy-MM-dd') and lower(`role`)='agent'
            ) t2 
            on t2.user_id=t1.user_id 
            group by t1.user_id
        ) detl
    ),
    -- 8、本月新增代理数
    new_agent_data as (
        select
            '{pt}' state_month, count(distinct t1.user_id) new_agent_cnt
        from
        (
            select
                user_id 
            from opay_dw_ods.ods_sqoop_base_user_upgrade_df 
            where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM')
        ) t1 
        inner join 
        (
            select 
                user_id
            from opay_dw_ods.ods_sqoop_base_user_df
            where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and lower(role)='agent'
        ) t2
        on t2.user_id=t1.user_id

    ),
    -- 9、本月成功交易代理数 本月总成功交易额-代理
    agent_trade_data as (
        select
            '{pt}' state_month, count(distinct t1.user_id) agent_trade_cnt, sum(t1.amount) agent_order_amt, count(t1.order_no) agent_order_cnt
        from
        (
            select
                user_id, amount, order_no
            from opay_dw_ods.ods_sqoop_base_big_order_df 
            where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM') and lower(order_status)='success'
        ) t1 
        inner join 
        (
            select 
                user_id
            from opay_dw_ods.ods_sqoop_base_user_df
            where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and lower(role)='agent'
        ) t2
        on t2.user_id=t1.user_id
    ),
    --10、总代理数
    agent_total_data as (
        select 
            '{pt}' state_month, count(distinct user_id) total_agent_cnt
        from opay_dw_ods.ods_sqoop_base_user_df 
        where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and role='agent'
    ),
    --13 本月商户交易情况  (ods_sqoop_base_merchant_order_df与big-order的区别以及这2张表中order_user_type区别)
    merchant_trade_data as (
        select
            '{pt}' state_month, sum(amount) merchant_order_amt, count(order_no) merchant_order_cnt
        from opay_dw_ods.ods_sqoop_base_merchant_order_df 
        where dt=date_format(last_day('{pt}'), 'yyyy-MM-dd') and date_format(create_time, 'yyyy-MM') = date_format('{pt}', 'yyyy-MM')
            and lower(order_status)='success' and order_user_type='MERCHANT'
    ),
    --14  本月非零余额账户数-用户+代理 /本月非零账户总金额-用户+代理 
    user_account_data as (
        select
            '{pt}' state_month, count(distinct user_id) user_account_cnt, sum(balance) user_account_amt
        from opay_dw_ods.ods_sqoop_base_account_user_df 
        where dt = date_format(last_day('{pt}'), 'yyyy-MM-dd') and balance > 0
    ),
    --15  本月非零余额账户数-商户/本月非零账户总金额-商户 
    merchant_account_data as (
        select
            '{pt}' state_month, count(distinct merchant_id) merchant_account_cnt, sum(balance) merchant_account_amt
        from opay_dw_ods.ods_sqoop_base_account_merchant_df 
        where dt = date_format(last_day('{pt}'), 'yyyy-MM-dd') and balance > 0
    ),
    --16 本月用户账户总金额-KYC1/2/3用户 
    kyc_account_data as (
        select
            '{pt}' state_month, sum(if(b.kyc_level=1, a.balance, 0)) kyc1_amt, sum(if(b.kyc_level=2, a.balance, 0)) kyc2_amt, sum(if(b.kyc_level=3, a.balance, 0)) kyc3_amt
        from 
        (
            select 
                user_id, balance
            from opay_dw_ods.ods_sqoop_base_account_user_df 
            where dt = date_format(last_day('{pt}'), 'yyyy-MM-dd')
        ) a
        join 
        (
            select 
                user_id, min(kyc_level) as kyc_level
            from opay_dw_ods.ods_sqoop_base_user_df 
            where dt = date_format(last_day('{pt}'), 'yyyy-MM-dd') 
            group by user_id
        ) b
        on a.user_id = b.user_id
    )
    insert overwrite table {table}
    partition(ym)
    select 
        t1.new_user_cnt, t2.recent_trade_user_cnt, t3.kyc1_user_cnt, t3.kyc2_user_cnt, t3.kyc3_user_cnt, 
        t4.aa_transfer_amt, t4.aa_transfer_cnt, t4.ac_transfer_amt, t4.ac_transfer_cnt, t4.airtime_amt, t4.airtime_cnt, t4.m_acquiring_amt, t4.m_acquiring_cnt, t4.tv_amt, t4.tv_cnt, t4.st_summary_amt, t4.st_summary_cnt, t5.agent_topup_amt, t5.agent_topup_cnt, t6.new_agent_cnt, t7.agent_trade_cnt, t7.agent_order_amt, t7.agent_order_cnt,
        t8.total_agent_cnt, t9.merchant_order_amt, t9.merchant_order_cnt, t10.user_account_cnt, t10.user_account_amt, 
        t11.merchant_account_cnt, t11.merchant_account_amt, t12.kyc1_amt, t12.kyc2_amt, t12.kyc3_amt,
        date_format('{pt}', 'yyyy-MM') ym
    from new_user_data t1
    join recently_trade_data t2 on t1.state_month=t2.state_month
    join kyc_user_data t3 on t1.state_month=t3.state_month
    join big_order_trade_data t4 on t1.state_month=t4.state_month
    join agent_topup_data t5 on t1.state_month=t5.state_month
    join new_agent_data t6 on t1.state_month=t6.state_month
    join agent_trade_data t7 on t1.state_month=t7.state_month
    join agent_total_data t8 on t1.state_month=t8.state_month
    join merchant_trade_data t9 on t1.state_month=t9.state_month
    join user_account_data t10 on t1.state_month=t10.state_month
    join merchant_account_data t11 on t1.state_month=t11.state_month
    join kyc_account_data t12 on t1.state_month=t12.state_month
    '''.format(
        pt='{{ds}}',
        table=table_name
    ),
    schema='opay',
    dag=dag
)
##---- hive operator end ---##

##----- 生成success文件 -----##
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        hdfs_data_dir=hdfs_path + '/dt={{ds}}'  ## 这里如何获取月份
    ),
    dag=dag)
##----- 生成success文件 -----##

dependence_ods_sqoop_base_user_df >> app_opay_cbn_report_m_task >> touchz_data_success