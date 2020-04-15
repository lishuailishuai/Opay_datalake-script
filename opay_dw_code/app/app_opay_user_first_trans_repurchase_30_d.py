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
    'start_date': datetime(2019, 9, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_user_first_trans_repurchase_30_d',
                  schedule_interval="00 03 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##


dwm_opay_user_first_trans_repurchase_di_prev_day_task = OssSensor(
    task_id='dwm_opay_user_first_trans_repurchase_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_first_trans_repurchase_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_user_first_trans_repurchase_30_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


##---- hive operator ---##
def app_opay_user_first_trans_repurchase_30_d_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false

    insert overwrite table {db}.{table} partition(country_code, dt)
        
    select 
      first_trans_date, first_sub_consume_scenario, if_first_trans_over_1w, sub_consume_scenario, 
      count(distinct if(gap_day = 0, user_id, null)) user_cnt0,
      count(distinct if(gap_day = 1, user_id, null)) user_cnt1,
      count(distinct if(gap_day >= 1 and gap_day <= 7, user_id, null)) user_cnt7,
      count(distinct if(gap_day >= 1 and gap_day <= 15, user_id, null)) user_cnt15,
      count(distinct if(gap_day >= 1 and gap_day <= 30, user_id, null)) user_cnt30,
      country_code,
      '{pt}'
    from opay_dw.dwm_opay_user_first_trans_repurchase_di
    where dt between date_sub('{pt}', 30) and '{pt}' and first_trans_date between date_sub('{pt}', 30) and '{pt}'
    group by first_trans_date, first_sub_consume_scenario, sub_consume_scenario, if_first_trans_over_1w, country_code
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_user_first_trans_repurchase_30_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_user_first_trans_repurchase_30_d_task = PythonOperator(
    task_id='app_opay_user_first_trans_repurchase_30_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwm_opay_user_first_trans_repurchase_di_prev_day_task >> app_opay_user_first_trans_repurchase_30_d_task