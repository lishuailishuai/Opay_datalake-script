# -*- coding: utf-8 -*-
"""
oride 客户端广告数据
"""
import airflow
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from airflow.sensors import OssSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
import time
import logging
from airflow.models import Variable
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from plugins.CountriesPublicFrame import CountriesPublicFrame

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 11, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_client_advertising_d',
    schedule_interval="00 02 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_client_advertising_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖 ufile://opay-datalake/oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal/dt=2019-08-09/hour=06'
    dependence_dwd_oride_client_event_detail_hi = UFileSensor(
        task_id="dependence_dwd_oride_client_event_detail_hi",
        filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
            pt='{{ ds }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    dependence_dwd_oride_client_event_detail_hi = OssSensor(
        task_id="dependence_dwd_oride_client_event_detail_hi",
        bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
            pt='{{ ds }}'
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

    msg = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}".format(pt=ds), "timeout": "300"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

# create_table = HiveOperator(
#    task_id='create_table_task',
#    hql="""
#        CREATE TABLE IF NOT EXISTS app_oride_client_advertising_d (
#            event_name string comment '广告事件名称',
#            pv int comment '广告PV量',
#            uv int comment '广告UV量'
#        )
#        partitioned by (
#            dt string comment '数据产出日期'
#        )
#        STORED AS ORC
#        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_client_advertising_d'
#    """,
#    schema="oride_dw",
#    dag=dag
# )

def app_oride_client_advertising_d_sql_task(ds):
    HQL = '''
    SET mapreduce.job.queuename=root.airflow;
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(dt)
    SELECT  
            CASE 
                WHEN event_name='homepage_window_show' THEN '首页弹窗广告展示'
                WHEN event_name='homepage_window_click' THEN '首页弹窗广告点击' 
                WHEN event_name='homepage_window_close_click' THEN '首页弹窗广告点击关闭' 
                WHEN event_name='banner_window_show' THEN 'banner广告展示' 
                WHEN event_name='banner_window_click' THEN 'banner广告点击'
                WHEN event_name='banner_window_close_click' THEN 'banner广告点击关闭' 
	            WHEN event_name='top_activities_click' THEN '点击ORide首页活动中心' 
	            WHEN event_name='top_activities_show' THEN '活动中心展示' 
	            WHEN event_name='activities_picture_click' THEN '在活动中心点击广告图' 
	            WHEN event_name='finished_window_show' THEN '支付完成页弹窗展示' 
	            WHEN event_name='finished_window_click' THEN '支付完成页弹窗点击' 
	            WHEN event_name='finishede_window_close_click' THEN '支付完成页弹窗点击关闭' 
	            ELSE '其他' END, 
            count(1) AS pv, 
            count(distinct ip) AS uv, 
            dt 
        FROM oride_dw.dwd_oride_client_event_detail_hi 
        WHERE dt = '{pt}' AND 
            event_name IN (
                'homepage_window_show',
                'homepage_window_click',
                'homepage_window_close_click',
                'banner_window_show',
                'banner_window_click',
                'banner_window_close_click',
                'top_activities_click',
                'top_activities_show',
                'activities_picture_click',
                'finished_window_show',
                'finished_window_click',
                'finishede_window_close_click'
            )
        GROUP BY dt, event_name;
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()
    """
            #功能函数
            alter语句: alter_partition
            删除分区: delete_partition
            生产success: touchz_success

            #参数
            第一个参数true: 所有国家是否上线。false 没有
            第二个参数true: 数据目录是有country_code分区。false 没有
            第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

            #读取sql
            %_sql(ds,v_hour)

            第一个参数ds: 天级任务
            第二个参数v_hour: 小时级任务，需要使用

        """
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "false", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_client_advertising_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_oride_client_advertising_d_task = PythonOperator(
    task_id='app_oride_client_advertising_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dwd_oride_client_event_detail_hi >> app_oride_client_advertising_d_task
