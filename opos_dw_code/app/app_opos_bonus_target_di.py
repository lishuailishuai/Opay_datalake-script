# -*- coding: utf-8 -*-
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
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_bonus_target_di',
                  schedule_interval="10 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，ods_sqoop_base_opos_bonus_record_di表，ufile://opay-datalake/opos_dw_sqoop/opay_crm/bd_admin_users
ods_sqoop_base_opos_bonus_record_di_task = UFileSensor(
    task_id='ods_sqoop_base_opos_bonus_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_admin_users",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区，dim_opos_bd_relation_df表，ufile://opay-datalake/opos/opos_dw/dim_opos_bd_relation_df
dim_opos_bd_relation_df_task = UFileSensor(
    task_id='dim_opos_bd_relation_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dim_opos_bd_relation_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_bonus_target_di"
hdfs_path = "ufile://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_opos_bonus_target_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    --02.先取红包record表
    insert overwrite table opos_dw.{table} partition(country_code,dt)
    select
    create_date,
    city_code,
    city_name,
    cm_id,
    cm_name,
    rm_id,
    rm_name,
    bdm_id,
    bdm_name,
    bd_id,
    bd_name,

    --已入账金额
    sum(if(status=1,bonus_amount,0)) as entered_account_amt,
    --待入账金额
    sum(if(status=0,bonus_amount,0)) as tobe_entered_account_amt,
    --被扫者奖励金额
    sum(bonus_amount) as swepted_award_amt,
    --已结算金额
    sum(if(status=1 and settle_status=1,bonus_amount,0)) as settled_amount,
    --待结算金额
    sum(if(status=1 and settle_status=0,bonus_amount,0)) as tobe_settled_amount,
    --获取金额的被扫者
    count(distinct(provider_account)) as swepted_award_cnt,

    --扫描红包二维码次数
    count(1) as sweep_times,
    --扫描红包二维码人数
    count(distinct(opay_account)) as sweep_people,
    --主扫红包金额
    sum(amount) as sweep_amt,

    --红包使用率
    sum(use_amount)/sum(bonus_amount) as bonus_use_percent,
    --红包使用金额
    sum(use_amount) as bonus_order_amt,

    'nal' as country_code,
    '{pt}' as dt
    from
    (
    select
    o.id,
    o.city_id as city_code,
    o.bd_id,
    o.opay_account,
    o.provider_account,
    o.receiver_account,
    o.amount,
    o.use_amount,
    o.bonus_amount,
    o.status,
    o.settle_status,
    o.settle_type,
    substr(o.create_time,0,10) as create_date,
    o.dt,

    b.cm_id,
    b.cm_name,
    b.rm_id,
    b.rm_name,
    b.bdm_id,
    b.bdm_name,
    b.bd_name,

    c.country as country_code,
    c.name as city_name
    from
    (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}') as o
    left join
    (select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}') as b
    on
    o.bd_id=b.bd_id
    left join
    (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as c
    on
    o.city_id=c.id
    ) as bonus_record
    group by
    create_date,
    country_code,
    city_code,
    city_name,
    cm_id,
    cm_name,
    rm_id,
    rm_name,
    bdm_id,
    bdm_name,
    bd_id,
    bd_name;


'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opos_bonus_target_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opos_bonus_target_di_task = PythonOperator(
    task_id='app_opos_bonus_target_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_opos_bonus_record_di_task >> app_opos_bonus_target_di_task
dim_opos_bd_relation_df_task >> app_opos_bonus_target_di_task

# 查看任务命令
# airflow list_tasks app_opos_bonus_target_di -sd /home/feng.yuan/app_opos_bonus_target_di.py
# 测试任务命令
# airflow test app_opos_bonus_target_di app_opos_bonus_target_di_task 2019-11-24 -sd /home/feng.yuan/app_opos_bonus_target_di.py

