# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lishuai',
    'start_date': datetime(2020, 3, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_ocredit_phones_risk_control_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_t_order_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


ods_sqoop_base_t_order_audit_history_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_audit_history_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order_audit_history",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


ods_sqoop_base_t_pay_order_df_task = OssSensor(
    task_id='ods_sqoop_base_t_pay_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_pay_order",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


ods_sqoop_base_t_contract_df_task = OssSensor(
    task_id='ods_sqoop_base_t_contract_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_contract",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

db_name = "ocredit_phones_dw"
table_name = "app_ocredit_phones_risk_control_cube_d"
hdfs_path = "oss://opay-datalake/ocredit_phones/ocredit_phones_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "ocredit_phones_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_ocredit_phones_risk_control_cube_d_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE ocredit_phones_dw.{table} partition(country_code,dt)



    select
          to_date(t2.Acreate_time) as date_of_entry,--`进件日期`,
          substr(to_date(t2.Acreate_time),1,7) as month_of_entry,--进件月
          concat(date_add(next_day(to_date(t2.Acreate_time),'MO'),-7),'_',date_add(next_day(to_date(t2.Acreate_time),'MO'),-1)) as week_of_entry,--进件周
          count(distinct t2.order_id) as entry_order_cnt,--`进件订单数`,
          count(distinct t2.opay_id) as entry_opay_cnt,--`进件用户数`,
          
          count(distinct case when t2.order_status not in ('10','12','13','99') then t2.opay_id else null end) as pre_amount,--`初审通过量`,
          count(distinct case when t2.order_status not in ('10','11','12','13','30','32','99') then t2.opay_id else null end) as review_amount,--`复审通过量`,
          
          count(distinct case when t2.order_status ='81' then t2.opay_id else null end) as loan_cnt,--放款量
          
          ---round(avg(case when t2.s10 is not null then t2.s1 else null end),2)as `进件-初审流转用时(分)`,
          avg(case when t2.s10 is not null then t2.s2 else null end) as pre_actual_average_minute, --`初审实际用时(分)`,
          ---round(avg(case when t2.s10 is not null then t2.s3 else null end),2) as `初审-复审流转用时(分)`,
          avg(case when t2.s10 is not null then t2.s4 else null end) as review_actual_average_minute,--`复审实际用时(分)`,
          ---round(avg(case when t2.s10 is not null then t2.s5 else null end),2) as `复审-支付流转用时(分)`,
          avg(case when t2.s10 is not null then t2.s6 else null end) as pay_actual_average_minute,--`支付实际用时(分)`,
          ---round(avg(case when t2.s10 is not null then t2.s7 else null end),2) as `支付-合同流转用时(分)`,
          avg(case when t2.s10 is not null then t2.s8 else null end) as contract_review_average_minute,--`合同审核实际用时(分)`,
          ---round(avg(case when t2.s10 is not null then t2.s9 else null end),2) as `合同审核-放款流转用时(分)`,
          avg(case when t2.s10 is not null then t2.s10 else null end) as total_use_average_minute,--`总用时(时)`
          
          'nal' as country_code,
          '{pt}' as dt
    
          from 
          (select 
          t1.Acreate_time,
          t1.opay_id,
          t1.order_id,
          t1.user_id,
          t1.terms,
          t1.order_status,
          ((unix_timestamp(t1.B1create_time) - unix_timestamp(t1.Acreate_time) ) / 60) as s1,      ----进件日初审流转时间
          ((unix_timestamp(t1.B1update_time) - unix_timestamp(t1.B1create_time) ) / 60) as s2,----初审实际时间
          (case when t1.B2create_time is not NULL then ((unix_timestamp(t1.B2create_time) - unix_timestamp(t1.B1update_time) ) / 60) else NULL end) as s3, ---初审结束-复审开始流转时间
          (case when t1.B2update_time is not NULL then ((unix_timestamp(t1.B2update_time) - unix_timestamp(t1.B2create_time) ) / 60) else NULL end) as s4,---复审实际时间
          (case when t1.Ccreate_time is not NULL then ((unix_timestamp(t1.Ccreate_time) - unix_timestamp(t1.B2update_time) ) / 60) else NULL end) as s5, ---复审结束-支付流转时间
          (case when t1.Cpay_time is not NULL then ((unix_timestamp(t1.Cpay_time) - unix_timestamp(t1.Ccreate_time) ) / 60) else NULL end) as s6,  --- 支付实际时间
          (case when t1.apply_time is not NULL and t1.Cpay_time is not NULL  then ((unix_timestamp(t1.apply_time) - unix_timestamp(t1.Cpay_time) ) / 60) else NULL end) as s7, --- 支付结束-合同审核流转时间
          (case when t1.apply_time is not NULL and t1.last_audit_time is not NULL then ((unix_timestamp(t1.last_audit_time) - unix_timestamp(t1.apply_time) ) / 60) else NULL end) as s8,--- 合同审核实际时间
          (case when t1.loan_time is not NULL and t1.last_audit_time is not NULL then ((unix_timestamp(t1.loan_time) - unix_timestamp(t1.last_audit_time) ) / 60) else NULL end) as s9,--- 合同通过到放款实际时间
          (case when t1.loan_time is not NULL then ((unix_timestamp(t1.loan_time) - unix_timestamp(t1.Acreate_time) ) / (3600)) else NULL end) as s10   --- 进件到放款实际时间
          from 
          (
          select 
          distinct
          cast(a.opay_id as string) `opay_id`,
          cast(a.order_id as string) `order_id`,
          cast(a.user_id as string) `user_id`,
          a.order_status,
          a.create_time as `Acreate_time`,   ---进件时间
          a.terms,
          a.down_payment,
          b1.create_time as `B1create_time`,  ---初审创建时间
          b1.update_time as `B1update_time`,  ---初审结束时间
          b2.create_time as `B2create_time`,  ---复审创建时间
          b2.update_time as `B2update_time`,  ---复审结束时间
          c.create_time as `Ccreate_time`,    ---支付创建时间
          c.pay_time as `Cpay_time`,          ---支付结束时间
          d.apply_time,                      ---合同审核开始时间
          d.last_audit_time,                 ---合同审核结束时间
          a.loan_time
          
          from 
          
          (select 
          opay_id,
          order_id,
          user_id,
          order_status,
          from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss') as create_time, --创建时间 
          from_unixtime(unix_timestamp(loan_time)+3600,'yyyy-MM-dd HH:mm:ss') as loan_time,--放款时间 
          terms,
          down_payment
          from ocredit_phones_dw_ods.ods_sqoop_base_t_order_df     ----主订单表，过滤掉测试数据
          where dt='{pt}'
          and to_date(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss'))>='2019-12-28'
          and user_id not in 
          (
          '1209783514507214849', 
          '1209126038292123650',
          '1210903150317494274',
          '1214471918163460097',
          '1215642304343425026',
          '1226878328587288578')
          and business_type = '0'
          ) as a
          
          left join 
          (
          select order_id,min(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss')) as create_time,
          max(from_unixtime(unix_timestamp(update_time)+3600,'yyyy-MM-dd HH:mm:ss')) as update_time
          from ocredit_phones_dw_ods.ods_sqoop_base_t_order_audit_history_df  ---审批记录表，1是初审
          where dt='{pt}'
          and audit_type='1'
          group by order_id
          )b1 on a.order_id=b1.order_id
          
          left join 
          (
          select order_id,min(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss')) as create_time,
          max(from_unixtime(unix_timestamp(update_time)+3600,'yyyy-MM-dd HH:mm:ss')) as update_time
          from ocredit_phones_dw_ods.ods_sqoop_base_t_order_audit_history_df  ---审批记录表，2是复审
          where dt='{pt}'
          and audit_type='2'
          group by order_id
          )b2 on a.order_id=b2.order_id
          
          left join
          (
          select order_id,min(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss')) as create_time,
          max(from_unixtime(unix_timestamp(pay_time)+3600,'yyyy-MM-dd HH:mm:ss')) as pay_time
          from ocredit_phones_dw_ods.ods_sqoop_base_t_pay_order_df   ---首付款支付信息表
          where dt='{pt}'
          and pay_type='0'
          and business_type='0'
          group by order_id
          ) as c on a.order_id=c.order_id
          
          left join 
          (
          select order_id,
          min(from_unixtime(unix_timestamp(apply_time)+3600,'yyyy-MM-dd HH:mm:ss')) as apply_time,
          max(from_unixtime(unix_timestamp(last_audit_time)+3600,'yyyy-MM-dd HH:mm:ss')) as last_audit_time,
          max(from_unixtime(unix_timestamp(final_audit_pass)+3600,'yyyy-MM-dd HH:mm:ss')) as final_audit_pass
          from ocredit_phones_dw_ods.ods_sqoop_base_t_contract_df     ---合同信息表，
          where dt='{pt}'
          and business_type='0'
          group by order_id
          )as d
          on a.order_id=d.order_id 
          )t1 
          )t2
          group by to_date(t2.Acreate_time),substr(to_date(t2.Acreate_time),1,7),concat(date_add(next_day(to_date(t2.Acreate_time),'MO'),-7),'_',date_add(next_day(to_date(t2.Acreate_time),'MO'),-1))
          grouping sets(
          to_date(t2.Acreate_time),substr(to_date(t2.Acreate_time),1,7),concat(date_add(next_day(to_date(t2.Acreate_time),'MO'),-7),'_',date_add(next_day(to_date(t2.Acreate_time),'MO'),-1))
          )



        '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 数据目录是有country_code分区。false 没有
        第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql_task(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    if datetime.strptime(ds, '%Y-%m-%d').weekday() == 6:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "false")
    else:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + app_ocredit_phones_risk_control_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


app_ocredit_phones_risk_control_cube_d_task = PythonOperator(
    task_id='app_ocredit_phones_risk_control_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_sqoop_base_t_order_df_task >> app_ocredit_phones_risk_control_cube_d_task
ods_sqoop_base_t_order_audit_history_df_task >> app_ocredit_phones_risk_control_cube_d_task
ods_sqoop_base_t_pay_order_df_task >> app_ocredit_phones_risk_control_cube_d_task
ods_sqoop_base_t_contract_df_task >> app_ocredit_phones_risk_control_cube_d_task