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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 1, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_bd_agent_cico_d',
                  schedule_interval="00 03 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##
dwd_opay_transfer_of_account_record_di_task = OssSensor(
    task_id='dwd_opay_transfer_of_account_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transfer_of_account_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_bd_admin_users_df_prev_day_task = OssSensor(
    task_id='ods_bd_admin_users_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_agent_crm/bd_admin_users",
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
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="opay_dw"
table_name="dwm_opay_bd_agent_cico_d"
hdfs_path="oss://opay-datalake/opay/opay_dw/"+table_name

##---- hive operator ---##
def dwm_opay_bd_agent_cico_d_sql_task(ds):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false
    with 
	agent_data as (
		select 
			id, cast(opay_id as string) as opay_id, bd_id, agent_status, updated_at
		from opay_dw_ods.ods_sqoop_base_bd_agent_df
		where dt = '${pt}'
	),
	bd_agent_data as (
		select 
			bd_id, 
			sum(if(agent_status = 1, 1, 0)) as audited_agent_cnt_his,
			sum(if(updated_at BETWEEN date_format(date_sub(date_format('${pt}', 'yyyy-MM-01'), 1), 'yyyy-MM-dd 23') AND date_format(last_day('${pt}'), 'yyyy-MM-dd 23') and agent_status = 1, 1, 0)) as audited_agent_cnt_m, 
			sum(if(updated_at BETWEEN date_format(date_sub(date_format('${pt}', 'yyyy-MM-01'), 1), 'yyyy-MM-dd 23') AND date_format(last_day('${pt}'), 'yyyy-MM-dd 23') and agent_status = 2, 1, 0)) as rejected_agent_cnt_m, 
			sum(if(updated_at BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23') and agent_status = 1, 1, 0)) as audited_agent_cnt, 
			sum(if(updated_at BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23') and agent_status = 2, 1, 0)) as rejected_agent_cnt
		from agent_data 
		where agent_status = 1 or agent_status = 2
		group by bd_id
	),
	bd_ci_data as (
		select
			t1.bd_id, 
			count(*) as ci_suc_order_cnt_m,
			sum(amount) as ci_suc_order_amt_m,
			sum(if(create_time BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23'), 1, 0)) as ci_suc_order_cnt,
			sum(if(create_time BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23'), amount, 0)) as ci_suc_order_amt
		from agent_data t1
		join (
			select 
				affiliate_id, amount, create_time, country_code
			from (
				select
					affiliate_id, amount, create_time, country_code, row_number() over(partition by order_no order by update_time) rn
				from opay_dw.dwd_opay_transfer_of_account_record_di
				where date_format(dt, 'yyyy-MM') = date_format('${pt}', 'yyyy-MM') and order_status = 'SUCCESS' and sub_service_type = 'Cash In'
			) t0 where rn = 1
		) ci on t1.opay_id = ci.affiliate_id
		group by t1.bd_id
	),
	bd_co_data as (
		select
			t1.bd_id, 
			count(*) as co_suc_order_cnt_m,
			sum(amount) as co_suc_order_amt_m,
			sum(if(create_time BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23'), 1, 0)) as co_suc_order_cnt,
			sum(if(create_time BETWEEN date_format(date_sub('${pt}', 1), 'yyyy-MM-dd 23') AND date_format('${pt}', 'yyyy-MM-dd 23'), amount, 0)) as co_suc_order_amt
		from agent_data t1
		join (
			select 
				originator_id, amount, create_time, country_code
			from (
				select
					originator_id, amount, create_time, country_code, row_number() over(partition by order_no order by update_time) rn
				from opay_dw.dwd_opay_transfer_of_account_record_di
				where date_format(dt, 'yyyy-MM') = date_format('${pt}', 'yyyy-MM') and order_status = 'SUCCESS' and sub_service_type = 'Cash Out'
			) t0 where rn = 1
		) co on 1.opay_id = co.originator_id
		group by t1.bd_id
	)
	insert overwrite {db}.{table}  partition(country_code='NG', dt='{pt}')
	select 
	    t10.bd_id as bd_admin_user_id,
	    nvl(t10.audited_agent_cnt_his, 0) as audited_agent_cnt_his, nvl(t10.audited_agent_cnt_m, 0) as audited_agent_cnt_m, 
	    nvl(t10.audited_agent_cnt, 0) as audited_agent_cnt, nvl(t10.rejected_agent_cnt_m, 0) as rejected_agent_cnt_m, 
	    nvl(t10.rejected_agent_cnt, 0) as rejected_agent_cnt, 
	    nvl(t11.ci_suc_order_cnt, 0) as ci_suc_order_cnt, nvl(t11.ci_suc_order_amt, 0) as ci_suc_order_amt, 
	    nvl(t11.ci_suc_order_cnt_m, 0) as ci_suc_order_cnt_m, nvl(t11.ci_suc_order_amt_m, 0) as ci_suc_order_amt_m,
	    nvl(t12.co_suc_order_cnt, 0) as co_suc_order_cnt, nvl(t12.co_suc_order_amt, 0) as co_suc_order_amt, 
	    nvl(t12.co_suc_order_cnt_m, 0) as co_suc_order_cnt_m, nvl(t12.co_suc_order_amt_m, 0) as co_suc_order_amt_m
	from bd_agent_data t10
	left join bd_ci_data t11 on t10.bd_id = t11.bd_id
	left join bd_co_data t12 on t10.bd_id = t12.bd_id
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

##---- hive operator end ---##

def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_bd_agent_cico_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")

dwm_opay_bd_agent_cico_d_task = PythonOperator(
    task_id='dwm_opay_bd_agent_cico_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transfer_of_account_record_di_task >> dwm_opay_bd_agent_cico_d_task