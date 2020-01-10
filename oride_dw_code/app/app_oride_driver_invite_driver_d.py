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
from airflow.sensors import UFileSensor
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.models import Variable


args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 5),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_invite_driver_d',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_invite_driver_d"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    dwd_oride_rider_signups_df_task = UFileSensor(
        task_id='dwd_oride_rider_signups_df_task',
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_rider_signups_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dim_oride_city_task = UFileSensor(
        task_id='dim_oride_city_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")

    dwd_oride_rider_signups_df_task = OssSensor(
        task_id='dwd_oride_rider_signups_df_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_rider_signups_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dim_oride_city_task = OssSensor(
        task_id='dim_oride_city_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

def app_oride_driver_invite_driver_d_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        WITH base as(
            SELECT *,
            from_unixtime(create_time,'yyyy-MM-dd') as cte_time,
            from_unixtime(veri_time,'yyyy-MM-dd') as ver_time
            from oride_dw.dwd_oride_rider_signups_df
            where dt='{pt}' and know_orider in(7,13,14)
        )
        
        INSERT OVERWRITE table {db}.{table} partition(country_code,dt)
        
        SELECT create_table.cte_time,
            t3.city_name,
            create_table.product_id,
            create_table.know_orider,
            create_table.submit_data_num,
            nvl(veri_table.audit_num,0)as audit_num,
            nvl(veri_table.audit_success_num,0)as audit_success_num,
            -10000 as finish_driver_num,
            'nal' as country_code,
            '{pt}' as dt
        from(
            SELECT cte_time,
            city,
            driver_type as product_id,--业务线
            know_orider,--渠道
            count( driver_id) as submit_data_num--提交资料人数
            from base 
            where cte_time='{pt}'
            GROUP BY cte_time,city,driver_type,know_orider
        )as create_table
        LEFT JOIN 
        (
            SELECT 
            ver_time,--查看日
            city,--城市名
            driver_type as product_id,--业务线
            know_orider,--渠道
            count( if(status in (1,2,9),driver_id,null)) as audit_num,--现场审核人数
            count( if(status=2,driver_id,null)) as audit_success_num--审核通过人数
            from base
            where ver_time='{pt}'
            GROUP BY ver_time,city,driver_type,know_orider
        )as veri_table
        on create_table.cte_time=veri_table.ver_time
        and create_table.city=veri_table.city
        and create_table.product_id=veri_table.product_id
        and create_table.know_orider=veri_table.know_orider
        LEFT JOIN
        (
            SELECT city_id,city_name
            from oride_dw.dim_oride_city 
            WHERE dt='{pt}'
        )as t3
        on create_table.city=t3.city_id;
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
    check_sql ='''
        select count(1)-count(distinct commit_day,regional_name,product_id,know_orider) as cnt
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
    _sql=app_oride_driver_invite_driver_d_sql_task(ds)

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

app_oride_driver_invite_driver_d_task = PythonOperator(
    task_id='app_oride_driver_invite_driver_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_rider_signups_df_task>>app_oride_driver_invite_driver_d_task
dim_oride_city_task>>app_oride_driver_invite_driver_d_task
