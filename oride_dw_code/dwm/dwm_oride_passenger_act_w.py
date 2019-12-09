# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import airflow
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
import logging
from airflow.hooks.hive_hooks import HiveCliHook
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import os

args = {
    'owner':"chenghui",
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_passenger_act_w',
                  schedule_interval="00 02 * * 1",
                  default_args=args,
                  catchup=False
)

##----------------------------------------- 依赖 ---------------------------------------##

dependence_dwd_oride_order_base_include_test_di_prev_day_task = S3KeySensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-bi',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "dwm_oride_passenger_act_w"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    date = "{pt}".format(pt=ds)
    year = date[0:4]

    sql = "hive -e \"select weekofyear('{pt}');\"".format(pt=ds)
    week = os.popen(sql).read().strip("\n")

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dw="+year+"_"+week, "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##
def dwm_oride_passenger_act_w_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    
    insert overwrite table {db}.{table} partition(country_code,dw)
    
    select passenger_id,
        city_id,
        driver_serv_type,
        country_code,
        concat(substr(dt,1,4),'_',weekofyear(dt))as dw
    from oride_dw.dwd_oride_order_base_include_test_di
    where dt between '{pt}' and date_add('{pt}',6)
    and status in(4,5) and city_id<>999001 and driver_id<>1
    group by country_code,concat(substr(dt,1,4),'_',weekofyear(dt)),
        city_id,driver_serv_type,passenger_id;
    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_oride_passenger_act_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_oride_passenger_act_w_task = PythonOperator(
    task_id='dwm_oride_passenger_act_w_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_task>>dwm_oride_passenger_act_w_task