# -*- coding: utf-8 -*-
"""
小司管表
"""
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from utils.connection_helper import get_hive_cursor
import logging
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 6),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_data_group_df',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)



##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
ods_sqoop_base_data_driver_group_df_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_group_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_group",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="oride_dw"
table_name = "dwd_oride_driver_data_group_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
def dwd_oride_driver_data_group_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table {db}.{table} partition(country_code,dt)
            select id,--组ID
                group_name,--组名
                group_leader,--组长（小司管）
                group_leader_id,--组leader骑手ID
                source, --0.admin后台 1.activity
                'nal' as country_code, --国家码
                '{pt}' dt --日期
            from 
                oride_dw_ods.ods_sqoop_base_data_driver_group_df
            where
                dt='{pt}';
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()
    # 主键重复校验
    check_sql = '''
    select count(1)-count(distinct id)as cnt
    from {db}.{table}
    where dt='{pt}'
    and country_code in ('nal')
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

#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_oride_driver_data_group_df_sql_task(ds)

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

dwd_oride_driver_data_group_df_task=PythonOperator(
    task_id='dwd_oride_driver_data_group_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_driver_group_df_task>>dwd_oride_driver_data_group_df_task