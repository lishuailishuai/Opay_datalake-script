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
from airflow.sensors import OssSensor

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2020, 2, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_active_user_cube_d',
                  schedule_interval="30 02 * * *",
                  default_args=args)

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
ods_sqoop_base_user_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
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
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

table_name = "app_opay_active_user_cube_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_active_user_cube_d_sql_task(ds, ds_nodash):
    HQL = '''
    set  hive.exec.max.dynamic.partitions.pernode=1000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    DROP TABLE IF EXISTS test_db.user_base_cube_{date};
    DROP TABLE IF EXISTS test_db.tran_cube_{date};
    create table if not exists test_db.user_base_cube_{date} as 
    SELECT user_id, ROLE,mobile,state
     FROM
          (SELECT user_id, ROLE,mobile, state,row_number() over(partition BY user_id ORDER BY update_time DESC) rn
            FROM opay_dw_ods.ods_sqoop_base_user_di
           WHERE dt<='{pt}' ) t1
     WHERE rn = 1;
      create table if not exists test_db.tran_cube_{date} as
    select top_consume_scenario,a.user_id,dt,a.role,b.state
    from 
              ( select 
                    top_consume_scenario, originator_id user_id,dt,originator_role role
                from opay_dw.dwd_opay_transaction_record_di
                where dt>date_sub('{pt}',30) and dt<='{pt}' 
                      and date_format(create_time, 'yyyy-MM-dd') =dt
                      and originator_type = 'USER' and originator_id is not null and originator_id != ''
                group by originator_id,dt,top_consume_scenario,originator_role
                union all
                select 
                    top_consume_scenario, affiliate_id user_id,dt,affiliate_role role
                from opay_dw.dwd_opay_transaction_record_di
                where dt>date_sub('{pt}',30) and dt<='{pt}' 
                      and date_format(create_time, 'yyyy-MM-dd') =dt
                      and affiliate_type = 'USER' and affiliate_id is not null and affiliate_id != ''
                group by top_consume_scenario,affiliate_id,dt,affiliate_role
              ) a 
    inner join 
              test_db.user_base_cube_{date} b 
    on a.user_id=b.user_id;

    INSERT overwrite TABLE opay_dw.app_opay_active_user_cube_d partition (country_code,dt)
    select nvl(top_consume_scenario,'ALL') top_consume_scenario,
           nvl(role,'ALL') role,
           nvl(state,'ALL') state,
           count (distinct case when dt='{pt}' then user_id end) active_user_cnt_d,
           count (distinct case when dt>date_sub('{pt}',7) then user_id end) active_user_cnt_7d,
           count (distinct user_id) active_user_cnt_30d,
           'NG' country_code,
           '{pt}' as dt
    from test_db.tran_cube_{date}
    group by top_consume_scenario,
             role,
             state
    with cube;
    DROP TABLE IF EXISTS test_db.user_base_cube_{date};
    DROP TABLE IF EXISTS test_db.tran_cube_{date};

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        date=ds_nodash
    )
    return HQL


def execution_data_task_id(ds, ds_nodash, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_active_user_cube_d_sql_task(ds, ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opay_active_user_cube_d_task = PythonOperator(
    task_id='app_opay_active_user_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task >> app_opay_active_user_cube_d_task
ods_sqoop_base_user_di_prev_day_task >> app_opay_active_user_cube_d_task


