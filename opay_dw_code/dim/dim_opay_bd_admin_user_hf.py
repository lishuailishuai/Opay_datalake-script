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
from plugins.CountriesAppFrame import CountriesAppFrame
from utils.get_local_time import GetLocalTime

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 27),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_opay_bd_admin_user_hf',
                  schedule_interval="25 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dim_opay_bd_admin_user_hf"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查上一个小时的本地时间依赖
dim_opay_bd_admin_user_hf_pre_locale_task = OssSensor(
    task_id='dim_opay_bd_admin_user_hf_pre_locale_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_admin_user_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=-1),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=-1)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查当前小时的分区依赖
ods_opay_bd_admin_user_hi_check_task = OssSensor(
    task_id='ods_opay_bd_admin_user_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_agent_crm_binlog.opay_agent_crm.bd_admin_users",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,execution_date,**op_kwargs):

    dag_ids=dag.dag_id

    #监控国家
    v_country_code='NG'

    #时间偏移量
    v_gap_hour=0

    v_date=GetLocalTime("opay",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['date']
    v_hour=GetLocalTime("opay",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['hour']

    #小时级监控
    tb_hour_task = [
        {"dag":dag,"db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,pt=v_date,now_hour=v_hour), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dim_opay_bd_admin_user_hf_sql_task(ds, v_date):
    HQL = '''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    with ods_bd_admin_hi as (
        SELECT 
            id as bd_admin_user_id, 
            username as bd_admin_user_name, 
            mobile as bd_admin_mobile,
            job_id as bd_admin_job_id, 
            leader_id as bd_admin_leader_id, 
            opay_user,
            staff_code, 
            name, 
            password, 
            department_id, 
            email, 
            status, 
            remember_token, 
            avatar,
            fcm_token, 
            create_user_id as bd_admin_create_user_id, 
            update_user_id as bd_admin_update_user_id,
            created_at as create_time,
            updated_at as update_time,    
            row_number() over(partition by id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
        from opay_dw_ods.ods_binlog_base_bd_admin_users_hi 
        where concat(dt, " ", hour) = date_format('{v_date}', 'yyyy-MM-dd HH') and `__deleted` = 'false'
    )
    insert overwrite table {db}.{table} partition (country_code, dt, hour)

    select 
        bd_admin_user_id, 
            bd_admin_user_name, 
            bd_admin_mobile,
            bd_admin_job_id, 
            bd_admin_leader_id, 
            opay_user,
            staff_code, 
            name, 
            password, 
            department_id, 
            email, 
            status, 
            remember_token, 
            avatar,
            fcm_token, 
            bd_admin_create_user_id, 
            bd_admin_update_user_id,
            create_time,
            update_time,
        date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour,
        country_code,
        date_format(default.localTime("{config}", country_code, '{v_date}', 0), 'yyyy-MM-dd') as dt,
        date_format(default.localTime("{config}", country_code, '{v_date}', 0), 'HH') as hour
    from (
        select 
            bd_admin_user_id, 
            bd_admin_user_name, 
            bd_admin_mobile,
            bd_admin_job_id, 
            bd_admin_leader_id, 
            opay_user,
            staff_code, 
            name, 
            password, 
            department_id, 
            email, 
            status, 
            remember_token, 
            avatar,
            fcm_token, 
            bd_admin_create_user_id, 
            bd_admin_update_user_id,
            create_time,
            update_time,
            country_code,
            row_number() over(partition by bd_admin_user_id order by update_time desc) rn
        from (
            SELECT 
                bd_admin_user_id, 
                bd_admin_user_name, 
                bd_admin_mobile,
                bd_admin_job_id, 
                bd_admin_leader_id, 
                opay_user,
                staff_code, 
                name, 
                password, 
                department_id, 
                email, 
                status, 
                remember_token, 
                avatar,
                fcm_token, 
                bd_admin_create_user_id, 
                bd_admin_update_user_id,
                create_time,
                update_time,
                country_code
            from opay_dw.dim_opay_bd_admin_user_hf 
            where concat(dt, " ", hour) between default.minLocalTimeRange("{config}", '{v_date}', -1) and default.maxLocalTimeRange("{config}", '{v_date}', -1) 
                and utc_date_hour = from_unixtime(cast(unix_timestamp('{v_date}', 'yyyy-MM-dd HH') - 3600 as BIGINT), 'yyyy-MM-dd HH')
            union all
            SELECT 
                bd_admin_user_id, 
                bd_admin_user_name, 
                bd_admin_mobile,
                bd_admin_job_id, 
                bd_admin_leader_id, 
                opay_user,
                staff_code, 
                name, 
                password, 
                department_id, 
                email, 
                status, 
                remember_token, 
                avatar,
                fcm_token, 
                bd_admin_create_user_id, 
                bd_admin_update_user_id,    
                default.localTime("{config}", 'NG', concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)), 0) as create_time,
                default.localTime("{config}", 'NG', concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)), 0) as update_time,
                'NG' AS country_code
            from ods_bd_admin_hi where rn = 1
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


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
            alter语句: alter_partition()
            删除分区: delete_partition()
            生产success: touchz_success()

        #参数
            is_countries_online --是否开通多国家业务 默认(true 开通)
            db_name --hive 数据库的名称
            table_name --hive 表的名称
            data_oss_path --oss 数据目录的地址
            is_country_partition --是否有国家码分区,[默认(true 有country_code分区)]
            is_result_force_exist --数据是否强行产出,[默认(true 必须有数据才生成_SUCCESS)] false 数据没有也生成_SUCCESS 
            execute_time --当前脚本执行时间(%Y-%m-%d %H:%M:%S)
            is_hour_task --是否开通小时级任务,[默认(false)]
            frame_type --模板类型(只有 is_hour_task:'true' 时生效): utc 产出分区为utc时间，local 产出分区为本地时间,[默认(utc)]。

        #读取sql
            %_sql(ds,v_hour)

    """

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "true",
            "execute_time": v_date,
            "is_hour_task": "true",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)



    # 读取sql
    _sql="\n"+cf.alter_partition()+"\n"+dim_opay_bd_admin_user_hf_sql_task(ds, v_date)


    logging.info('Executing: %s',_sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生产success
    cf.touchz_success()


dim_opay_bd_admin_user_hf_task = PythonOperator(
    task_id='dim_opay_bd_admin_user_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dim_opay_bd_admin_user_hf_pre_locale_task >> dim_opay_bd_admin_user_hf_task
ods_opay_bd_admin_user_hi_check_task >> dim_opay_bd_admin_user_hf_task

