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
    'start_date': datetime(2019, 11, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_opos_bd_relation_df',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，ods_sqoop_base_bd_admin_users_df表，ufile://opay-datalake/opos_dw_sqoop/opay_crm/bd_admin_users
ods_sqoop_base_bd_admin_users_df_task = UFileSensor(
    task_id='ods_sqoop_base_bd_admin_users_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_admin_users",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区，ods_sqoop_base_bd_shop_df表，ufile://opay-datalake/opos_dw_sqoop/opay_crm/bd_shop
ods_sqoop_base_bd_shop_df_task = UFileSensor(
    task_id='ods_sqoop_base_bd_shop_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_shop",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区，ods_sqoop_base_bd_city_df表,ufile://opay-datalake/opos_dw_sqoop/opay_crm/bd_city
ods_sqoop_base_bd_city_df_task = UFileSensor(
    task_id='ods_sqoop_base_bd_city_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_city",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "dim_opos_bd_relation_df"
hdfs_path = "ufile://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids), "partition": "dt={pt}".format(pt=ds),
         "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dim_opos_bd_relation_df_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    --01.首先清空组织机构层级临时表,然后插入最新的组织机构层级数据
    --插入最新的数据
    insert overwrite table opos_dw.dim_opos_bd_info_df partition(country_code,dt)
    select 
    b.hcm_id,
    b.hcm_name,
    b.cm_id,
    b.cm_name,
    b.rm_id,
    b.rm_name,
    b.bdm_id,
    b.bdm_name,
    b.bd_id,
    b.bd_name,
    'nal' as country_code,
    '{pt}' as dt
    from
    (
    select hcm.id as hcm_id,hcm.name as hcm_name,cm.id as cm_id,cm.name as cm_name,rm.id as rm_id,rm.name as rm_name,bdm.id as bdm_id,bdm.name as bdm_name,bd.id as bd_id,bd.name as bd_name 
    from 
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 6) bd
    left join
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 5) bdm 
    on bd.leader_id = bdm.id
    left join
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 4) rm
    on bdm.leader_id = rm.id
    left join
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 3) cm 
    on rm.leader_id = cm.id
    left join
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 2) hcm
    on cm.leader_id = hcm.id
    ) as b;

    --02.取出所有商户信息，关联bd
    insert overwrite table opos_dw.dim_opos_bd_relation_df partition(country_code,dt)
    select 
    b.hcm_id,
    b.hcm_name,
    b.cm_id,
    b.cm_name,
    b.rm_id,
    b.rm_name,
    b.bdm_id,
    b.bdm_name,
    b.bd_id,
    b.bd_name,

    s.id as shop_id,
    s.opay_id as opay_id,
    s.shop_name as shop_name,
    s.opay_account as opay_account,

    s.city_id as city_code,
    s.name as city_name,
    'nal' as country_code,
    '{pt}' as dt
    from 
    --取出所有商铺，以商铺为主键，先关联地市，后关联bd信息表
    (
      select shop.id,shop.shop_name,shop.bd_id,shop.opay_id,shop.opay_account,shop.city_id,city.country,city.name 
      from 
      (select id,shop_name,bd_id,opay_id,opay_account,city_id from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}') as shop 
      left join 
      (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as city 
      on 
      shop.city_id=city.id
    ) as s
    left join
    --关联bd_id各个层级的信息
    (select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}') as b
    on
    s.bd_id=b.bd_id;

'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct id) as cnt
      FROM {db}.{table}
      WHERE country_code='nal' and dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_opos_bd_relation_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_opos_bd_relation_df_task = PythonOperator(
    task_id='dim_opos_bd_relation_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_bd_admin_users_df_task >> dim_opos_bd_relation_df_task
ods_sqoop_base_bd_shop_df_task >> dim_opos_bd_relation_df_task
ods_sqoop_base_bd_city_df_task >> dim_opos_bd_relation_df_task



