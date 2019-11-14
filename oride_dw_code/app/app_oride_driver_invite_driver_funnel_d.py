# -*- coding: utf-8 -*-
"""
司机邀请司机数据表
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_invite_driver_funnel_d',
                  schedule_interval="00 4 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dim_oride_driver_audit_base_task = UFileSensor(
    task_id='dim_oride_driver_audit_base_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_audit_base",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dim_oride_city_task = HivePartitionSensor(
    task_id="dim_oride_city_task",
    table="dim_oride_city",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwm_oride_driver_base_di_task = UFileSensor(
    task_id='dwm_oride_driver_base_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_invite_driver_funnel_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_invite_driver_funnel_d_sql_task(ds):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        INSERT OVERWRITE table {db}.{table} partition(country_code,dt)
        SELECT 
            a.commit_week,--提交周
            c.city_name as regional_name,--地域名
            if(a.product_id is NOT NULL,a.product_id,0) as product_id,--业务线
            a.know_orider,--渠道
            count(DISTINCT a.driver_id) as submit_data_num,--提交资料人数
            count(if(a.status in (1,2,9),a.driver_id,null)) as audit_num,--现场审核人数
            count(if(a.status=2,a.driver_id,null)) as audit_success_num,--审核通过人数
            count(DISTINCT if(d.is_finish_driver=1,a.driver_id,null)) as finish_driver_num, --完单司机数量
            'nal' as country_code,
            '{pt}' as dt
        from
        (
            SELECT *,weekofyear(from_unixtime(create_time,'yyyy-MM-dd')) as commit_week
            from oride_dw.dim_oride_driver_audit_base
            where dt='{pt}' and know_orider in(7,13,14) and driver_id!=0
        ) as a 
        LEFT JOIN 
        (
            SELECT city_id,city_name
            from oride_dw.dim_oride_city 
            WHERE dt='{pt}'
        ) as c
        on a.city_id=c.city_id
        LEFT JOIN
        (
            SELECT driver_id,
                is_finish_driver --是否完单司机标志
            from oride_dw.dwm_oride_driver_base_di
            where dt='{pt}'
        ) as d
        on a.driver_id=d.driver_id
        GROUP BY a.commit_week,c.city_name,a.product_id,a.know_orider 
        GROUPING SETS(
            (a.commit_week,c.city_name,a.product_id,a.know_orider),
            (a.commit_week,c.city_name,a.know_orider)
        );
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
        select count(1)-count(distinct commit_week,regional_name,product_id,know_orider) as cnt
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


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_driver_invite_driver_funnel_d_sql_task(ds)

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


app_oride_driver_invite_driver_funnel_d_task = PythonOperator(
    task_id='app_oride_driver_invite_driver_funnel_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_oride_driver_audit_base_task >> app_oride_driver_invite_driver_funnel_d_task
dim_oride_city_task >> app_oride_driver_invite_driver_funnel_d_task
dwm_oride_driver_base_di_task >> app_oride_driver_invite_driver_funnel_d_task