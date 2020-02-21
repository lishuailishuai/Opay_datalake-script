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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 2, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_data_report',
                  schedule_interval="00 02 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_data_report"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dwm_oride_driver_base_df_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwm_oride_order_base_di_prev_day_task = OssSensor(
        task_id='dwm_oride_order_base_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_driver_forbidden_record_df_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_forbidden_record_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_forbidden_record_df/country_code=nal",
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
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_data_report_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(country_code,dt)
    select dri.driver_id,
           if(forbid.driver_id is not null,1,0) as is_forbidden_login, --是否被禁止登录【如果该表产出很晚的话，就不沉淀在数仓了】
           dri_dur.driver_online_dur_30days, --司机近30天总在线时长
           dri_dur.driver_onjob_days, --近30天司机在职天数
           dri.driver_show_order_cnt, --当天成功被推送订单量（骑手端show节点）
           dri.driver_request_order_cnt, --当天司机接单量，即应答单量
           dri.driver_finish_order_cnt, --当天司机完单量
           ord.driver_after_cancel_cnt, --应答后司机取消单量
           ord.passanger_after_cancel_cnt,  --应答后乘客取消单量
           ord.peak_finish_cnt,  --高峰完单量
           ord.evaluated_ord_total_score,  --被评价订单总得分
           ord.evaluated_ord_cnt, --被评价订单量
           dri.group_id, --所属组id
           dri_group.group_name, --所属组名字
           dri.city_id, --城市id
           cit.city_name, --城市名称
           dri.product_id, --业务线
           dri.country_code, --国家编码
           dri.dt  --日期
    from (select *        
    from oride_dw.dwm_oride_driver_base_df
    where dt='{pt}') dri
    left join
    (select driver_id
    from oride_dw.dwd_oride_driver_forbidden_record_df
    where dt='{pt}' 
    and role=1 
    and status=0
    group by driver_id) forbid
    on dri.driver_id=forbid.driver_id  
    left join
    (select driver_id,sum(nvl(driver_service_dur,0)+nvl(driver_cannel_pick_dur,0)+nvl(driver_free_dur,0)) as driver_online_dur_30days, --司机近30天总在线时长
    count(1) as driver_onjob_days --近30天司机在职天数       
    from oride_dw.dwm_oride_driver_base_df
    where datediff('{pt}',dt)>=0 and datediff('{pt}',dt)<30 
    and fault<6
    group by driver_id) dri_dur
    on dri.driver_id=dri_dur.driver_id
    left join
    (select driver_id,
           sum(is_driver_after_cancel) as driver_after_cancel_cnt, --应答后司机取消单量
           sum(is_passanger_after_cancel) as passanger_after_cancel_cnt,  --应答后乘客取消单量
           sum(if(is_peak=1 and is_finish=1,1,0)) as peak_finish_cnt,  --高峰完单量
           sum(nvl(score,0)) as evaluated_ord_total_score,  --被评价订单总得分
           sum(if(nvl(score,0)>0,1,0)) as evaluated_ord_cnt --被评价订单量
    from oride_dw.dwm_oride_order_base_di
    where dt='{pt}'
    group by driver_id) ord
    on dri.driver_id=ord.driver_id
    left join 
    (select id,group_leader,group_name,group_leader_id 
     from oride_dw.dwd_oride_driver_data_group_df 
     where dt='{pt}') dri_group 
     on dri.group_id=dri_group.id
     left join
     (select * 
      from oride_dw.dim_oride_city
      where dt='{pt}') cit
      on dri.city_id=cit.city_id;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
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
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_driver_data_report_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_driver_data_report_task = PythonOperator(
    task_id='app_oride_driver_data_report_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_driver_base_df_prev_day_task >> app_oride_driver_data_report_task
dwm_oride_order_base_di_prev_day_task >> app_oride_driver_data_report_task
dwd_oride_driver_forbidden_record_df_prev_day_task >> app_oride_driver_data_report_task

