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
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 2, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_opay_bd_agent_base_hf',
                  schedule_interval="03 * * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dim_opay_bd_agent_base_hf"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("utc_locale_time_config"))
time_zone = config['NG']['time_zone']


##----------------------------------------- 依赖 ---------------------------------------##
### 检查上一个小时的本地时间依赖
dim_opay_terminal_base_hf_pre_locale_task = OssSensor(
    task_id='dim_opay_terminal_base_hf_pre_locale_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_terminal_base_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(time_zone=time_zone,gap_hour=-1),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(time_zone=time_zone,gap_hour=-1)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
### 检查当前小时的分区依赖
ods_opay_terminal_base_hi_check_task = OssSensor(
        task_id='ods_opay_terminal_base_hi_check_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="opay_binlog/opay_merchant_overlord_recon_db.opay_overlord.terminal",
            pt='{{ds}}',
            hour='{{ execution_date.strftime("%H") }}'
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




def dim_opay_bd_agent_base_hf_sql_task(ds, v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (country_code, dt, hour)
    
    select 
        id,
        agent_name,
        agent_surname,
        first_name,
        agent_middle_name,
        agent_address,
        agent_gender,
        country,
        agent_email,
        opay_account,
        user_id,
        agent_bvn,
        agent_state,
        birthday,
        phone,
        agent_passport,
        government_issued_photo,
        bill_photo,
        lng,
        lat,
        fence_id,
        city_id,
        agent_status,
        create_id,
        modify_id,
        bd_admin_user_id,
        agent_check_id,
        created_time,
        updated_time,
        date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour,
        country_code,
        date_format(default.localTime("{config}", country_code, '{v_date}', 0), 'yyyy-MM-dd') as dt,
        date_format(default.localTime("{config}", country_code, '{v_date}', 0), 'HH') as hour
    from (
        select 
            id,
            agent_name,
            agent_surname,
            first_name,
            agent_middle_name,
            agent_address,
            agent_gender,
            country,
            agent_email,
            opay_account,
            user_id,
            agent_bvn,
            agent_state,
            birthday,
            phone,
            agent_passport,
            government_issued_photo,
            bill_photo,
            lng,
            lat,
            fence_id,
            city_id,
            agent_status,
            create_id,
            modify_id,
            bd_admin_user_id,
            agent_check_id,
            created_time,
            updated_time,
            country_code,
            row_number() over(partition by user_id order by update_time desc) rn
        from (
            SELECT 
                id,
                agent_name,
                agent_surname,
                first_name,
                agent_middle_name,
                agent_address,
                agent_gender,
                country,
                agent_email,
                opay_account,
                user_id,
                agent_bvn,
                agent_state,
                birthday,
                phone,
                agent_passport,
                government_issued_photo,
                bill_photo,
                lng,
                lat,
                fence_id,
                city_id,
                agent_status,
                create_id,
                modify_id,
                bd_admin_user_id,
                agent_check_id,
                created_time,
                updated_time,
                country_code
            from opay_dw.dim_opay_bd_agent_base_hf 
            where concat(dt, " ", hour) between default.minLocalTimeRange("{config}", '{v_date}', -1) and default.maxLocalTimeRange("{config}", '{v_date}', -1) 
                and utc_date_hour = from_unixtime(cast(unix_timestamp('{v_date}', 'yyyy-MM-dd HH') - 3600 as BIGINT), 'yyyy-MM-dd HH')
            union all
            SELECT 
                id,
                agent_name,
                agent_surname,
                first_name,
                agent_middle_name,
                agent_address,
                agent_gender,
                country,
                agent_email,
                opay_account,
                user_id,
                agent_bvn,
                agent_state,
                birthday,
                phone,
                agent_passport,
                government_issued_photo,
                bill_photo,
                lng,
                lat,
                fence_id,
                city_id,
                agent_status,
                create_id,
                modify_id,
                bd_admin_user_id,
                agent_check_id,
                default.localTime("{config}", 'NG', created_at, 0) as create_time,
                default.localTime("{config}", 'NG', updated_at, 0) as update_time,
                'NG' AS country_code
            from opay_dw_ods.ods_binlog_base_user_hi 
            where concat(dt, " ", hour) = date_format('{v_date}', 'yyyy-MM-dd HH') and `__deleted` = 'false'
        ) t0 
    ) t1 where rn = 1
    
    
    '''.format(
        pt=ds,
        v_date=v_date,
        table=table_name,
        db=db_name,
        config=config

    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_opay_bd_agent_base_hf_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_opay_bd_agent_base_hf_task = PythonOperator(
    task_id='dim_opay_bd_agent_base_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)



