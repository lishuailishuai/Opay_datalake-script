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

dag = airflow.DAG('dim_opay_user_base_di',
                  schedule_interval="00 03 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##
#依赖前一天分区
ods_sqoop_base_user_upgrade_df_task = UFileSensor(
    task_id='ods_sqoop_base_user_upgrade_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_ods/opay_user/user_upgrade",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 


ods_sqoop_base_user_email_di_task = UFileSensor(
    task_id='ods_sqoop_base_user_email_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user_email",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 


ods_sqoop_base_user_di_task = UFileSensor(
    task_id='ods_sqoop_base_user_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
table_name="dim_opay_user_base_di"
hdfs_path="ufile://opay-datalake/opay/opay_dw/"+table_name

##---- hive operator ---##
fill_dim_opay_user_base_di_task = HiveOperator(
    task_id='fill_dim_opay_user_base_di_task',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false

    alter table dim_opay_user_base_di drop partition(country_code='NG',dt='{pt}');

    alter table dim_opay_user_base_di add partition(country_code='NG',dt='{pt}');

    insert overwrite table dim_opay_user_base_di partition(country_code, dt)
    select 
        t1.id,
        t1.user_id,
        t1.mobile,
        t1.business_name,
        t1.first_name,
        t1.middle_name,
        t1.surname,
        t1.kyc_level,
        t1.kyc_update_time,
        if(length(t1.bvn)=0, '-', replace(t1.bvn, '\t', '')) bvn,
        t1.dob bithday,
        t1.gender,
        t1.country,
        t1.state,
        t1.city,
        t1.address,
        t1.lga,
        t1.role,
        t1.referral_code,
        t1.referrer_code,
        t1.notification,
        if(t2.role='agent' and t2.upgrade_status='upgraded', t2.upgrade_date, '9999-01-01 00:00:00') agent_upgrade_time,
        t3.email,
        if(t3.email_verified='Y', 'authed', 'unauth') email_auth_status,
        t3.update_time email_auth_time,
        t1.create_time,
        t1.update_time,
        case t1.country
            when 'Nigeria' then 'NG'
            when 'Norway' then 'NO'
            when 'Ghana' then 'GH'
            when 'Botswana' then 'BW'
            when 'Ghana' then 'GH'
            when 'Kenya' then 'KE'
            when 'Malawi' then 'MW'
            when 'Mozambique' then 'MZ'
            when 'Poland' then 'PL'
            when 'South Africa' then 'ZA'
            when 'Sweden' then 'SE'
            when 'Tanzania' then 'TZ'
            when 'Uganda' then 'UG'
            when 'USA' then 'US'
            when 'Zambia' then 'ZM'
            when 'Zimbabwe' then 'ZW'
            else 'NG'
            end as country_code,
        t1.dt
    from  opay_dw_ods.ods_sqoop_base_user_di t1
    left join 
    (
        select user_id, role, upgrade_type, upgrade_status, upgrade_date
        from opay_dw_ods.ods_sqoop_base_user_upgrade_df
        where dt='{pt}'
    ) t2 on t1.user_id = t2.user_id 
    left join
    (
        select * from (
            select user_id, email, email_verified, update_time,
            row_number() over(partition by user_id order by update_time desc) rn
            from opay_dw_ods.ods_sqoop_base_user_email_di
        ) mail_temp where rn = 1

    ) t3 on t1.user_id = t3.user_id

    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator ---##
# dim_opay_user_base_di_task = HiveOperator(
#     task_id='dim_opay_user_base_di_task',
#     hql='''
#     set hive.exec.dynamic.partition.mode=nonstrict;
#     set hive.mapjoin.smalltable.filesize=128000000; --default 25000000
#     set hive.exec.parallel=true; --default false
#     set mapreduce.map.memory.mb=4000; --default 3072
#     insert overwrite table dim_opay_user_base_di 
#     partition(country_code, dt)
#     select 
#         user_di.id,
#         user_di.user_id,
#         user_di.mobile,
#         user_di.business_name,
#         user_di.first_name,
#         user_di.middle_name,
#         user_di.surname,
#         user_di.kyc_level,
#         user_di.kyc_update_time,
#         user_di.bvn,
#         user_di.dob bithday,
#         user_di.gender,
#         user_di.country,
#         user_di.state,
#         user_di.city,
#         user_di.address,
#         user_di.lga,
#         user_di.role,
#         user_di.referral_code,
#         user_di.referrer_code,
#         user_di.notification,
        
#         if(upgrade_di.role='agent' and upgrade_di.upgrade_status='upgraded', upgrade_di.upgrade_date, '9999-01-01 00:00:00') agent_upgrade_time,
#         email_di.email,
#         if(email_di.email_verified='Y', 'authed', 'unauth') email_auth_status,
#         email_di.update_time email_auth_time,
        
#         user_di.create_time,
#         user_di.update_time,
#         case user_di.country
#             when 'Nigeria' then 'NG'
#             when 'Norway' then 'NO'
#             when 'Ghana' then 'GH'
#             when 'Botswana' then 'BW'
#             when 'Ghana' then 'GH'
#             when 'Kenya' then 'KE'
#             when 'Malawi' then 'MW'
#             when 'Mozambique' then 'MZ'
#             when 'Poland' then 'PL'
#             when 'South Africa' then 'ZA'
#             when 'Sweden' then 'SE'
#             when 'Tanzania' then 'TZ'
#             when 'Uganda' then 'UG'
#             when 'USA' then 'US'
#             when 'Zambia' then 'ZM'
#             when 'Zimbabwe' then 'ZW'
#             else 'NG'
#             end as country_code,
#         '{pt}' dt
#     from
#     (
#         select 
#             t2.*
#         from
#         (
#             select 
#                 user_id 
#             from opay_dw_ods.ods_sqoop_base_user_di 
#             where dt = '{pt}'
#             union
#             select 
#                 user_id
#             from opay_dw_ods.ods_sqoop_base_user_email_di
#             where dt = '{pt}'
#             union
#             select 
#                 user_id
#             from opay_dw_ods.ods_sqoop_base_user_upgrade_df
#             where dt = '{pt}' and (date_format(create_time, 'yyyy-MM-dd') = {pt} or date_format(update_time, 'yyyy-MM-dd') = {pt})
#         ) t1
#         join
#         (
#             select * from (
#                 select *, row_number() over(partition by user_id order by update_time desc) rn 
#                 from opay_dw_ods.ods_sqoop_base_user_di
#             ) user_temp where rn = 1
#         ) t2 on t1.user_id = t2.user_id
#     ) user_di
#     left join
#     (
#         select
#             user_id, email, email_verified, update_time
#         from opay_dw_ods.ods_sqoop_base_user_email_di
#         where dt = '{pt}'
#     ) email_di on user_di.user_id = email_di.user_id
#     left join
#     (
#          select 
#             user_id, role, upgrade_type, upgrade_status, upgrade_date
#         from opay_dw_ods.ods_sqoop_base_user_upgrade_df
#         where dt = '{pt}' and (date_format(create_time, 'yyyy-MM-dd') = {pt} or date_format(update_time, 'yyyy-MM-dd') = {pt})
#     ) upgrade_di on user_di.user_id = upgrade_di.user_id

#     '''.format(
#         pt='{{ds}}'
#     ),
#     schema='opay_dw',
#     dag=dag
# )
##---- hive operator end ---##

#生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/country_code=NG/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_user_upgrade_df_task >>fill_dim_opay_user_base_di_task
ods_sqoop_base_user_email_di_task>>fill_dim_opay_user_base_di_task
ods_sqoop_base_user_di_task >> fill_dim_opay_user_base_di_task
fill_dim_opay_user_base_di_task>> touchz_data_success