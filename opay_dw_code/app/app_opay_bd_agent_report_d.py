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
    'start_date': datetime(2019, 9, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_bd_agent_report_d',
                  schedule_interval="00 03 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##
app_opay_bd_agent_cico_d_task = OssSensor(
    task_id='app_opay_bd_agent_cico_d_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/app_opay_bd_agent_cico_d/country_code=NG",
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
table_name="app_opay_bd_agent_report_d"
hdfs_path="oss://opay-datalake/opay/opay_dw/"+table_name

##---- hive operator ---##
def app_opay_bd_agent_report_d_sql_task(ds):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false
    with 
	base_date as (
		select * from app_opay_bd_agent_cico_d where dt = '{pt}'
	),
	bd_data as (
		select * from base_data where bd_admin_job_id = 6
	),
	bdm_base_data as (
		select * from base_data where bd_admin_job_id = 5
	)
	bdm_data as (
		select 
			t0.bd_admin_user_id, t0.bd_admin_user_name, t0.bd_admin_user_mobile, t0.bd_admin_dept_id, t0.bd_admin_job_id, t0.bd_admin_leader_id,
			sum(t0.audited_agent_cnt_his + nvl(t1.sub_audited_agent_cnt_his, 0)) as audited_agent_cnt_his,
			sum(t0.audited_agent_cnt_m + nvl(t1.sub_audited_agent_cnt_m, 0)) as audited_agent_cnt_m,
			sum(t0.audited_agent_cnt + nvl(t1.sub_audited_agent_cnt, 0)) as audited_agent_cnt,
			sum(t0.rejected_agent_cnt_m + nvl(t1.sub_rejected_agent_cnt_m, 0)) as rejected_agent_cnt_m,
			sum(t0.rejected_agent_cnt + nvl(t1.sub_rejected_agent_cnt, 0)) as rejected_agent_cnt,
			sum(t0.ci_suc_order_cnt + nvl(t1.sub_ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
			sum(t0.ci_suc_order_amt + nvl(t1.sub_ci_suc_order_amt, 0)) as ci_suc_order_amt,
			sum(t0.ci_suc_order_cnt_m + nvl(t1.sub_ci_suc_order_cnt_m, 0)) as ci_suc_order_cnt_m,
			sum(t0.ci_suc_order_amt_m + nvl(t1.sub_ci_suc_order_amt_m, 0)) as ci_suc_order_amt_m,
			sum(t0.co_suc_order_cnt + nvl(t1.sub_co_suc_order_cnt, 0)) as co_suc_order_cnt,
			sum(t0.co_suc_order_amt + nvl(t1.sub_co_suc_order_amt, 0)) as co_suc_order_amt,
			sum(t0.co_suc_order_cnt_m + nvl(t1.sub_co_suc_order_cnt_m, 0)) as co_suc_order_cnt_m,
			sum(t0.co_suc_order_amt_m + nvl(t1.sub_co_suc_order_amt_m, 0)) as co_suc_order_amt_m
		from bdm_base_data t0
		left join (
			select 
				bd_admin_leader_id, 
				sum(audited_agent_cnt_his) as sub_audited_agent_cnt_his, 
				sum(audited_agent_cnt_m) sub_audited_agent_cnt_m, 
				sum(audited_agent_cnt) sub_audited_agent_cnt,
				sum(rejected_agent_cnt_m) sub_rejected_agent_cnt_m,
				sum(rejected_agent_cnt) sub_rejected_agent_cnt,
				sum(ci_suc_order_cnt) sub_ci_suc_order_cnt,
				sum(ci_suc_order_amt) sub_ci_suc_order_amt,
				sum(ci_suc_order_cnt_m) sub_ci_suc_order_cnt_m,
				sum(ci_suc_order_amt_m) sub_ci_suc_order_amt_m,
				sum(co_suc_order_cnt) sub_co_suc_order_cnt,
				sum(co_suc_order_amt) sub_co_suc_order_amt,
				sum(co_suc_order_cnt_m) sub_co_suc_order_cnt_m,

				sum(co_suc_order_amt_m) sub_co_suc_order_amt_m
			from bd_data 
			group by bd_admin_leader_id
		) t1 on t1.bd_admin_leader_id = t0.bd_admin_user_id
		
	),
	rm_base_data as (
		select * from base_data where bd_admin_job_id = 4
	),
	rm_data as (
		select 
			t0.bd_admin_user_id, t0.bd_admin_user_name, t0.bd_admin_user_mobile, t0.bd_admin_dept_id, t0.bd_admin_job_id, t0.bd_admin_leader_id,
			sum(t0.audited_agent_cnt_his + nvl(t1.sub_audited_agent_cnt_his, 0)) as audited_agent_cnt_his,
			sum(t0.audited_agent_cnt_m + nvl(t1.sub_audited_agent_cnt_m, 0)) as audited_agent_cnt_m,
			sum(t0.audited_agent_cnt + nvl(t1.sub_audited_agent_cnt, 0)) as audited_agent_cnt,
			sum(t0.rejected_agent_cnt_m + nvl(t1.sub_rejected_agent_cnt_m, 0)) as rejected_agent_cnt_m,
			sum(t0.rejected_agent_cnt + nvl(t1.sub_rejected_agent_cnt, 0)) as rejected_agent_cnt,
			sum(t0.ci_suc_order_cnt + nvl(t1.sub_ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
			sum(t0.ci_suc_order_amt + nvl(t1.sub_ci_suc_order_amt, 0)) as ci_suc_order_amt,
			sum(t0.ci_suc_order_cnt_m + nvl(t1.sub_ci_suc_order_cnt_m, 0)) as ci_suc_order_cnt_m,
			sum(t0.ci_suc_order_amt_m + nvl(t1.sub_ci_suc_order_amt_m, 0)) as ci_suc_order_amt_m,
			sum(t0.co_suc_order_cnt + nvl(t1.sub_co_suc_order_cnt, 0)) as co_suc_order_cnt,
			sum(t0.co_suc_order_amt + nvl(t1.sub_co_suc_order_amt, 0)) as co_suc_order_amt,
			sum(t0.co_suc_order_cnt_m + nvl(t1.sub_co_suc_order_cnt_m, 0)) as co_suc_order_cnt_m,
			sum(t0.co_suc_order_amt_m + nvl(t1.sub_co_suc_order_amt_m, 0)) as co_suc_order_amt_m
		from rm_base_data t0
		left join (
			select 
				bd_admin_leader_id, 
				sum(audited_agent_cnt_his) as sub_audited_agent_cnt_his, 
				sum(audited_agent_cnt_m) sub_audited_agent_cnt_m, 
				sum(audited_agent_cnt) sub_audited_agent_cnt,
				sum(rejected_agent_cnt_m) sub_rejected_agent_cnt_m,
				sum(rejected_agent_cnt) sub_rejected_agent_cnt,
				sum(ci_suc_order_cnt) sub_ci_suc_order_cnt,
				sum(ci_suc_order_amt) sub_ci_suc_order_amt,
				sum(ci_suc_order_cnt_m) sub_ci_suc_order_cnt_m,
				sum(ci_suc_order_amt_m) sub_ci_suc_order_amt_m,
				sum(co_suc_order_cnt) sub_co_suc_order_cnt,
				sum(co_suc_order_amt) sub_co_suc_order_amt,
				sum(co_suc_order_cnt_m) sub_co_suc_order_cnt_m,
				sum(co_suc_order_amt_m) sub_co_suc_order_amt_m
			from bdm_data 
			group by bd_admin_leader_id
		) t1 on t1.bd_admin_leader_id = t0.bd_admin_user_id
	),
	cm_base_data as (
		select * from base_data where bd_admin_job_id = 3
	),
	cm_data as (
		select 
			t0.bd_admin_user_id, t0.bd_admin_user_name, t0.bd_admin_user_mobile, t0.bd_admin_dept_id, t0.bd_admin_job_id, t0.bd_admin_leader_id,
			sum(t0.audited_agent_cnt_his + nvl(t1.sub_audited_agent_cnt_his, 0)) as audited_agent_cnt_his,
			sum(t0.audited_agent_cnt_m + nvl(t1.sub_audited_agent_cnt_m, 0)) as audited_agent_cnt_m,
			sum(t0.audited_agent_cnt + nvl(t1.sub_audited_agent_cnt, 0)) as audited_agent_cnt,
			sum(t0.rejected_agent_cnt_m + nvl(t1.sub_rejected_agent_cnt_m, 0)) as rejected_agent_cnt_m,
			sum(t0.rejected_agent_cnt + nvl(t1.sub_rejected_agent_cnt, 0)) as rejected_agent_cnt,
			sum(t0.ci_suc_order_cnt + nvl(t1.sub_ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
			sum(t0.ci_suc_order_amt + nvl(t1.sub_ci_suc_order_amt, 0)) as ci_suc_order_amt,
			sum(t0.ci_suc_order_cnt_m + nvl(t1.sub_ci_suc_order_cnt_m, 0)) as ci_suc_order_cnt_m,
			sum(t0.ci_suc_order_amt_m + nvl(t1.sub_ci_suc_order_amt_m, 0)) as ci_suc_order_amt_m,
			sum(t0.co_suc_order_cnt + nvl(t1.sub_co_suc_order_cnt, 0)) as co_suc_order_cnt,
			sum(t0.co_suc_order_amt + nvl(t1.sub_co_suc_order_amt, 0)) as co_suc_order_amt,
			sum(t0.co_suc_order_cnt_m + nvl(t1.sub_co_suc_order_cnt_m, 0)) as co_suc_order_cnt_m,
			sum(t0.co_suc_order_amt_m + nvl(t1.sub_co_suc_order_amt_m, 0)) as co_suc_order_amt_m
		from cm_base_data t0
		left join (
			select 
				bd_admin_leader_id, 
				sum(audited_agent_cnt_his) as sub_audited_agent_cnt_his, 
				sum(audited_agent_cnt_m) sub_audited_agent_cnt_m, 
				sum(audited_agent_cnt) sub_audited_agent_cnt,
				sum(rejected_agent_cnt_m) sub_rejected_agent_cnt_m,
				sum(rejected_agent_cnt) sub_rejected_agent_cnt,
				sum(ci_suc_order_cnt) sub_ci_suc_order_cnt,
				sum(ci_suc_order_amt) sub_ci_suc_order_amt,
				sum(ci_suc_order_cnt_m) sub_ci_suc_order_cnt_m,
				sum(ci_suc_order_amt_m) sub_ci_suc_order_amt_m,
				sum(co_suc_order_cnt) sub_co_suc_order_cnt,
				sum(co_suc_order_amt) sub_co_suc_order_amt,
				sum(co_suc_order_cnt_m) sub_co_suc_order_cnt_m,
				sum(co_suc_order_amt_m) sub_co_suc_order_amt_m
			from rm_data 
			group by bd_admin_leader_id
		) t1 on t1.bd_admin_leader_id = t0.bd_admin_user_id
	),
	hcm_base_data as (
		select * from base_data where bd_admin_job_id = 2
	),
	hcm_data as (
		select 
			t0.bd_admin_user_id, t0.bd_admin_user_name, t0.bd_admin_user_mobile, t0.bd_admin_dept_id, t0.bd_admin_job_id, t0.bd_admin_leader_id,
			sum(t0.audited_agent_cnt_his + nvl(t1.sub_audited_agent_cnt_his, 0)) as audited_agent_cnt_his,
			sum(t0.audited_agent_cnt_m + nvl(t1.sub_audited_agent_cnt_m, 0)) as audited_agent_cnt_m,
			sum(t0.audited_agent_cnt + nvl(t1.sub_audited_agent_cnt, 0)) as audited_agent_cnt,
			sum(t0.rejected_agent_cnt_m + nvl(t1.sub_rejected_agent_cnt_m, 0)) as rejected_agent_cnt_m,
			sum(t0.rejected_agent_cnt + nvl(t1.sub_rejected_agent_cnt, 0)) as rejected_agent_cnt,
			sum(t0.ci_suc_order_cnt + nvl(t1.sub_ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
			sum(t0.ci_suc_order_amt + nvl(t1.sub_ci_suc_order_amt, 0)) as ci_suc_order_amt,
			sum(t0.ci_suc_order_cnt_m + nvl(t1.sub_ci_suc_order_cnt_m, 0)) as ci_suc_order_cnt_m,
			sum(t0.ci_suc_order_amt_m + nvl(t1.sub_ci_suc_order_amt_m, 0)) as ci_suc_order_amt_m,
			sum(t0.co_suc_order_cnt + nvl(t1.sub_co_suc_order_cnt, 0)) as co_suc_order_cnt,
			sum(t0.co_suc_order_amt + nvl(t1.sub_co_suc_order_amt, 0)) as co_suc_order_amt,
			sum(t0.co_suc_order_cnt_m + nvl(t1.sub_co_suc_order_cnt_m, 0)) as co_suc_order_cnt_m,
			sum(t0.co_suc_order_amt_m + nvl(t1.sub_co_suc_order_amt_m, 0)) as co_suc_order_amt_m
		from hcm_base_data t0
		left join (
			select 
				bd_admin_leader_id, 
				sum(audited_agent_cnt_his) as sub_audited_agent_cnt_his, 
				sum(audited_agent_cnt_m) sub_audited_agent_cnt_m, 
				sum(audited_agent_cnt) sub_audited_agent_cnt,
				sum(rejected_agent_cnt_m) sub_rejected_agent_cnt_m,
				sum(rejected_agent_cnt) sub_rejected_agent_cnt,
				sum(ci_suc_order_cnt) sub_ci_suc_order_cnt,
				sum(ci_suc_order_amt) sub_ci_suc_order_amt,
				sum(ci_suc_order_cnt_m) sub_ci_suc_order_cnt_m,
				sum(ci_suc_order_amt_m) sub_ci_suc_order_amt_m,
				sum(co_suc_order_cnt) sub_co_suc_order_cnt,
				sum(co_suc_order_amt) sub_co_suc_order_amt,
				sum(co_suc_order_cnt_m) sub_co_suc_order_cnt_m,
				sum(co_suc_order_amt_m) sub_co_suc_order_amt_m
			from cm_data 
			group by bd_admin_leader_id
		) t1 on t1.bd_admin_leader_id = t0.bd_admin_user_id
	),
	pic_base_data as (
		select * from base_data where bd_admin_job_id = 1
	),
	pic_data as (
		select 
			t0.bd_admin_user_id, t0.bd_admin_user_name, t0.bd_admin_user_mobile, t0.bd_admin_dept_id, t0.bd_admin_job_id, t0.bd_admin_leader_id,
			sum(t0.audited_agent_cnt_his + nvl(t1.sub_audited_agent_cnt_his, 0)) as audited_agent_cnt_his,
			sum(t0.audited_agent_cnt_m + nvl(t1.sub_audited_agent_cnt_m, 0)) as audited_agent_cnt_m,
			sum(t0.audited_agent_cnt + nvl(t1.sub_audited_agent_cnt, 0)) as audited_agent_cnt,
			sum(t0.rejected_agent_cnt_m + nvl(t1.sub_rejected_agent_cnt_m, 0)) as rejected_agent_cnt_m,
			sum(t0.rejected_agent_cnt + nvl(t1.sub_rejected_agent_cnt, 0)) as rejected_agent_cnt,
			sum(t0.ci_suc_order_cnt + nvl(t1.sub_ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
			sum(t0.ci_suc_order_amt + nvl(t1.sub_ci_suc_order_amt, 0)) as ci_suc_order_amt,
			sum(t0.ci_suc_order_cnt_m + nvl(t1.sub_ci_suc_order_cnt_m, 0)) as ci_suc_order_cnt_m,
			sum(t0.ci_suc_order_amt_m + nvl(t1.sub_ci_suc_order_amt_m, 0)) as ci_suc_order_amt_m,
			sum(t0.co_suc_order_cnt + nvl(t1.sub_co_suc_order_cnt, 0)) as co_suc_order_cnt,
			sum(t0.co_suc_order_amt + nvl(t1.sub_co_suc_order_amt, 0)) as co_suc_order_amt,
			sum(t0.co_suc_order_cnt_m + nvl(t1.sub_co_suc_order_cnt_m, 0)) as co_suc_order_cnt_m,
			sum(t0.co_suc_order_amt_m + nvl(t1.sub_co_suc_order_amt_m, 0)) as co_suc_order_amt_m
		from pic_base_data t0
		left join (
			select 
				bd_admin_leader_id, 
				sum(audited_agent_cnt_his) as sub_audited_agent_cnt_his, 
				sum(audited_agent_cnt_m) sub_audited_agent_cnt_m, 
				sum(audited_agent_cnt) sub_audited_agent_cnt,
				sum(rejected_agent_cnt_m) sub_rejected_agent_cnt_m,
				sum(rejected_agent_cnt) sub_rejected_agent_cnt,
				sum(ci_suc_order_cnt) sub_ci_suc_order_cnt,
				sum(ci_suc_order_amt) sub_ci_suc_order_amt,
				sum(ci_suc_order_cnt_m) sub_ci_suc_order_cnt_m,
				sum(ci_suc_order_amt_m) sub_ci_suc_order_amt_m,
				sum(co_suc_order_cnt) sub_co_suc_order_cnt,
				sum(co_suc_order_amt) sub_co_suc_order_amt,
				sum(co_suc_order_cnt_m) sub_co_suc_order_cnt_m,
				sum(co_suc_order_amt_m) sub_co_suc_order_amt_m
			from hcm_data 
			group by bd_admin_leader_id
		) t1 on t1.bd_admin_leader_id = t0.bd_admin_user_id
	)
	insert overwrite {db}.{table}  partition(country_code='NG', dt='{pt}')
	select * from bd_data
	union all
	select * from bdm_data
	union all
	select * from rm_data
	union all
	select * from cm_data
	union all
	select * from hcm_data
	union all
	select * from pic_data
        
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
    _sql = app_opay_bd_agent_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")

app_opay_bd_agent_report_d_task = PythonOperator(
    task_id='app_opay_bd_agent_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

app_opay_bd_agent_cico_d_task >> app_opay_bd_agent_report_d_task