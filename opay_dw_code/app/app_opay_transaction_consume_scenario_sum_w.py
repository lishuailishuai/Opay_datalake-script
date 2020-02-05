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
    'start_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_transaction_consume_scenario_sum_w',
                  schedule_interval="00 03 * * 1",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_transaction_record_di_prev_day_task = OssSensor(
    task_id='dwd_opay_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transaction_record_di/country_code=NG",
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
         "partition": "country_code=NG/dt={pt}".format(pt=airflow.macros.ds_add(ds, +6)), "timeout": "800"}
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
table_name = "app_opay_transaction_consume_scenario_sum_w"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


##---- hive operator ---##
def app_opay_transaction_consume_scenario_sum_w_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false

    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        top_consume_scenario, 
        sub_consume_scenario,
        originator_type, 
        originator_role, 
        client_source, 
        order_status,
        sum(amount) as order_amt, 
        count(*) as order_cnt,
        country_code,
        date_sub(next_day('{pt}', 'mo'), 1) as dt
    from (
        select 
            top_consume_scenario, 
            sub_consume_scenario, 
            originator_type, 
            originator_role, 
            client_source, 
            order_status,
            amount,
            country_code,
            row_number() over(partition by order_no order by update_time desc) rn
        from {db}.dwd_opay_transaction_record_di 
        where dt between date_sub(next_day('{pt}', 'mo'), 7)  and date_sub(next_day('{pt}', 'mo'), 1) 
            and create_time BETWEEN date_format(date_sub(next_day('{pt}', 'mo'), 8), 'yyyy-MM-dd 23') AND date_format(date_sub(next_day('{pt}', 'mo'), 1), 'yyyy-MM-dd 23') 
    ) t1 where rn = 1
    group by country_code, top_consume_scenario, sub_consume_scenario, originator_type, originator_role, client_source, order_status

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
    _sql = app_opay_transaction_consume_scenario_sum_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success('{pt}'.format(pt=airflow.macros.ds_add(ds, +6)), db_name, table_name, hdfs_path, "true", "true")


app_opay_transaction_consume_scenario_sum_w_task = PythonOperator(
    task_id='app_opay_transaction_consume_scenario_sum_w_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task >> app_opay_transaction_consume_scenario_sum_w_task