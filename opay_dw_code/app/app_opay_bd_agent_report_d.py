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
from plugins.CountriesAppFrame import CountriesAppFrame

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

dag = airflow.DAG('app_opay_bd_agent_report_d',
                  schedule_interval="30 02 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##
dwm_opay_bd_agent_trans_df_task = OssSensor(
    task_id='dwm_opay_bd_agent_trans_df_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_bd_agent_trans_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_bd_relation_df_task = OssSensor(
    task_id='dim_opay_bd_relation_df_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_relation_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_bd_admin_user_df_prev_day_task = OssSensor(
    task_id='dim_opay_bd_admin_user_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_admin_user_df/country_code=NG",
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
def app_opay_bd_agent_report_d_sql_task(ds, ds_nodash):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false
    create table if not exists test_db.bd_report_temp_{pt_str} as
            select 
                nvl(job_bd_user_id, 'ALL') as job_bd_user_id, 
                nvl(job_bdm_user_id, 'ALL') as job_bdm_user_id, 
                nvl(job_rm_user_id, 'ALL') as job_rm_user_id,
                nvl(job_cm_user_id, 'ALL') as job_cm_user_id,
                nvl(job_hcm_user_id, 'ALL') as job_hcm_user_id, 
                nvl(job_pic_user_id, 'ALL') as job_pic_user_id, 
                
                sum(audit_suc_cnt) as audited_agent_cnt_his, 
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', audit_suc_cnt, 0)) as audited_agent_cnt_m, 
                sum(if(t2.business_date = '{pt}', audit_suc_cnt, 0)) as audited_agent_cnt,
                
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', audit_fail_cnt, 0)) as rejected_agent_cnt_m,
                sum(if(t2.business_date = '{pt}', audit_fail_cnt, 0)) as rejected_agent_cnt,
        
                sum(if(t2.business_date = '{pt}', ci_suc_order_cnt, 0)) as ci_suc_order_cnt,
                sum(if(t2.business_date = '{pt}', ci_suc_order_amt, 0)) as ci_suc_order_amt,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', ci_suc_order_cnt, 0)) as ci_suc_order_cnt_m,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', ci_suc_order_amt, 0)) as ci_suc_order_amt_m,
                
                sum(if(t2.business_date = '{pt}', co_suc_order_cnt, 0)) as co_suc_order_cnt,
                sum(if(t2.business_date = '{pt}', co_suc_order_amt, 0)) as co_suc_order_amt,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', co_suc_order_cnt, 0)) as co_suc_order_cnt_m,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', co_suc_order_amt, 0)) as co_suc_order_amt_m,
                
                sum(bind_terminal_agent_cnt_his) as bind_terminal_agent_cnt_his,
                sum(bind_terminal_agent_cnt_m) as bind_terminal_agent_cnt_m,
                
                sum(if(t2.business_date = '{pt}', pos_suc_amt, 0)) as pos_suc_amt,
                sum(if(t2.business_date = '{pt}', pos_suc_cnt, 0)) as pos_suc_cnt,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', pos_suc_amt, 0)) as pos_suc_amt_m,
                sum(if(t2.business_date between date_format('{pt}', 'yyyy-MM-01') and '{pt}', pos_suc_cnt, 0)) as pos_suc_cnt_m,
                sum(pos_agent_cnt_m) as pos_agent_cnt_m,
                sum(pos_agent_cnt) as pos_agent_cnt
            from (
                select 
                    bd_admin_user_id, 
                    business_date, 
                    audit_suc_cnt, 
                    audit_fail_cnt,
                    ci_suc_order_cnt,
                    ci_suc_order_amt,
                    
                    co_suc_order_cnt,
                    co_suc_order_amt,
                    bind_terminal_agent_cnt_his,
                    bind_terminal_agent_cnt_m,
                    pos_suc_amt,
                    pos_suc_cnt,
                    pos_agent_cnt_m,
                    pos_agent_cnt
                    
                from opay_dw.dwm_opay_bd_agent_trans_df where dt = '{pt}'
            ) t1 join (
                select 
                    bd_admin_user_id, bd_admin_user_name, bd_admin_job_id, bd_admin_status, 
                    job_bd_user_id, job_bdm_user_id, job_rm_user_id, job_cm_user_id, job_hcm_user_id, job_pic_user_id,
                    dt as business_date
                from opay_dw.dim_opay_bd_relation_df
                where dt <= '{pt}'
            ) t2 on t1.bd_admin_user_id = t2.bd_admin_user_id and t1.business_date = t2.business_date
            group by job_bd_user_id, job_bdm_user_id, job_rm_user_id, job_cm_user_id, job_hcm_user_id, job_pic_user_id
            grouping sets(
                job_bd_user_id, job_bdm_user_id, job_rm_user_id, 
                job_cm_user_id, job_hcm_user_id, job_pic_user_id
            );
    with 
        bd_data as (
            select 
                bd_admin_user_id, bd_admin_user_name, bd_admin_mobile,
                department_id, bd_admin_job_id, bd_admin_leader_id
            from opay_dw.dim_opay_bd_admin_user_df 
            where dt = '{pt}' and bd_admin_job_id > 0
        ),
        cube_data as (
            select 
                job_bd_user_id, 
                job_bdm_user_id, 
                job_rm_user_id,
                job_cm_user_id,
                job_hcm_user_id, 
                job_pic_user_id, 
                audited_agent_cnt_his, 
                audited_agent_cnt_m, 
                audited_agent_cnt,
                rejected_agent_cnt_m,
                rejected_agent_cnt,
                ci_suc_order_cnt,
                ci_suc_order_amt,
                ci_suc_order_cnt_m,
                ci_suc_order_amt_m,
                co_suc_order_cnt,
                co_suc_order_amt,
                co_suc_order_cnt_m,
                co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from test_db.bd_report_temp_{pt_str}
        )
        insert overwrite table {db}.{table} partition(country_code='NG', dt='{pt}')
        select 
            bd.bd_admin_user_id, bd.bd_admin_user_name, bd.bd_admin_mobile, 
            bd.department_id, bd.bd_admin_job_id, bd.bd_admin_leader_id,
            audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
            rejected_agent_cnt_m, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
            co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
            bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
            pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
            pos_agent_cnt_m, pos_agent_cnt
        from (
            select 
                job_bd_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_bd_user_id != 'ALL' and job_bd_user_id != '-'
            union all
            select 
                job_bdm_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_bdm_user_id != 'ALL' and job_bdm_user_id != '-'
            union all
            select 
                job_rm_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_rm_user_id != 'ALL' and job_rm_user_id != '-'
            union all
             select 
                job_cm_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_cm_user_id != 'ALL' and job_cm_user_id != '-'
            union all
             select 
                job_hcm_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_hcm_user_id != 'ALL' and job_hcm_user_id != '-'
            union all
             select 
                job_pic_user_id as bd_admin_user_id, audited_agent_cnt_his, audited_agent_cnt_m, audited_agent_cnt,
                rejected_agent_cnt_m, rejected_agent_cnt,
                ci_suc_order_cnt, ci_suc_order_amt, ci_suc_order_cnt_m, ci_suc_order_amt_m,
                co_suc_order_cnt, co_suc_order_amt, co_suc_order_cnt_m, co_suc_order_amt_m,
                bind_terminal_agent_cnt_his, bind_terminal_agent_cnt_m,
                pos_suc_amt, pos_suc_cnt, pos_suc_amt_m, pos_suc_cnt_m, 
                pos_agent_cnt_m, pos_agent_cnt
            from cube_data 
            where job_pic_user_id != 'ALL' and job_pic_user_id != '-'
        ) report 
        left join bd_data bd on report.bd_admin_user_id = bd.bd_admin_user_id ;
        DROP TABLE IF EXISTS test_db.bd_report_temp_{pt_str}
        
       
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        pt_str=ds_nodash
    )
    return HQL

# 主流程
def execution_data_task_id(ds, ds_nodash, dag, **kwargs):
    v_execution_time = kwargs.get('v_execution_time')
    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_bd_agent_report_d_sql_task(ds, ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_bd_agent_report_d_task = PythonOperator(
    task_id='app_opay_bd_agent_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwm_opay_bd_agent_trans_df_task >> app_opay_bd_agent_report_d_task
dim_opay_bd_admin_user_df_prev_day_task >> app_opay_bd_agent_report_d_task
dim_opay_bd_relation_df_task >> app_opay_bd_agent_report_d_task