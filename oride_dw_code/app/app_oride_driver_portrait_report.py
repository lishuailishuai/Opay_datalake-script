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
    'start_date': datetime(2020, 2, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_portrait_report',
                  schedule_interval="00 02 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_portrait_report"
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

dwm_oride_driver_finance_di_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_finance_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_finance_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_abnormal_order_di_prev_day_task = OssSensor(
        task_id='dwd_oride_abnormal_order_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_abnormal_order_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dim_oride_driver_audit_base_prev_day_task = OssSensor(
        task_id='dim_oride_driver_audit_base_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_audit_base/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dim_oride_driver_base_prev_day_task = OssSensor(
        task_id='dim_oride_driver_base_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=NG",
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

def app_oride_driver_portrait_report_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(country_code,dt)
    select dwm_dri.driver_id, --司机ID
           nvl(fin.complain_amount,0)+nvl(ord.falsify_driver_cancel,0)+nvl(malice_brush_driver_deduct,0)+nvl(abnormal_ord.amount,0) as driver_falsify_amount, --司机罚款金额
           ord.driver_after_cancel_ord_cnt, --应答后司机取消单量
           dwm_dri.recent_finish_create_time, --司机最近完单时间 
           dwm_dri.newest_driver_version, --司机端最新版本（接单）
           dri_con.max_continuous_finish_days, --最近30天最大连续完单天数
           dri_con.acc_finish_days, --近30天累计完单天数
           dri_recruit.know_orider, --获取司机来源渠道
           dwm_dri.first_bind_time, --初次绑定时间 
           dwm_dri.end_service_time,--专车司机结束收份子钱时间
           fin.theory_start_date, --司机理论开始还款日期,即手机贷款首次还款日期
           fin.theory_end_date, --司机理论结束还款日期,即手机贷款结束还款日期 
           (fin.driver_falsify_num+ord.driver_falsify_num+abnormal_ord.driver_falsify_num) as driver_falsify_num, --司机当天被罚款次数
           dri_lea.driver_leave_date, --司机离职日期
           dwm_dri.country_code,  --国家编码
           dwm_dri.dt  --日期
    from (select driver_id,
           recent_finish_create_time, --司机最近完单时间   
           first_bind_time, --初次绑定时间 
           end_service_time,--专车司机结束收份子钱时间
           newest_driver_version, --司机端最新版本（接单）
           country_code,
           dt
           --driver_leave_date --司机离职日期，待新增
    from oride_dw.dwm_oride_driver_base_df
    where dt='{pt}') dwm_dri
    
    left join
    (select driver_id,
           complain_amount,  --司机被投诉罚款
           if(nvl(complain_amount,0)>0,1,0) as driver_falsify_num, --司机当天被罚款次数
           theory_start_date, --司机理论开始还款日期,即手机贷款首次还款日期
           date_add(theory_start_date,cast(numbers as int)) as theory_end_date --司机理论结束还款日期,即手机贷款结束还款日期      
    from oride_dw.dwm_oride_driver_finance_di
    where dt='{pt}') fin
    on dwm_dri.driver_id=fin.driver_id
    
    left join
    (select driver_id,
           sum(nvl(falsify_driver_cancel,0)) as falsify_driver_cancel, --司机取消罚款
           sum(nvl(malice_brush_driver_deduct,0)) as malice_brush_driver_deduct,  --恶意刷单司机扣款 
           sum(if(nvl(falsify_driver_cancel,0)>0 or nvl(malice_brush_driver_deduct,0)>0,1,0)) as driver_falsify_num, --司机当天被罚款次数
           sum(is_td_driver_after_cancel) as driver_after_cancel_ord_cnt  --应答后司机取消单量
    from oride_dw.dwd_oride_order_base_include_test_di
    WHERE dt = '{pt}'
    AND city_id<>'999001' --去除测试数据
    and driver_id<>1
    group by driver_id) ord
    on dwm_dri.driver_id=ord.driver_id
    
    left join
    (select driver_id,
           sum(nvl(amount,0)) as amount, --司机反作弊罚款
           sum(if(nvl(amount,0)>0,1,0)) as driver_falsify_num --司机当天被罚款次数
    from oride_dw.dwd_oride_abnormal_order_di
    where dt='{pt}'
    and substr(f_create_time,1,10)=dt
    group by driver_id) abnormal_ord
    on dwm_dri.driver_id=abnormal_ord.driver_id
    
    left join
    (select driver_id,
           know_orider --获取司机来源渠道
    from(select driver_id,
           know_orider, --获取司机来源渠道
           row_number() over(partition by driver_id order by id desc) as rn
    from oride_dw.dim_oride_driver_audit_base
    where dt='{pt}') t
    where t.rn=1) dri_recruit
    on dwm_dri.driver_id=dri_recruit.driver_id
    
    left join
    --司机最大连续完单天数，累计完单天数
    (
    select driver_id,
           max(continuous_finish_days) as max_continuous_finish_days, --最近30天最大连续完单天数
           sum(continuous_finish_days) as acc_finish_days --近30天累计完单天数
    from(select driver_id,
           from_day,
           min(dt) start_date, -- 连续完单的开始时间
           max(dt) end_date, -- 连续完单的结束时间   
           count(1) continuous_finish_days -- 连续完单的天数
    from (select driver_id,dt,date_sub(dt,row_number() over(partition by driver_id order by dt)) as from_day
    from oride_dw.dwm_oride_driver_base_df
    where datediff('{pt}',dt)>=0 and datediff('{pt}',dt)<30
    and is_td_finish=1) t
    group by driver_id,
           from_day) m
    group by driver_id) dri_con
    on dwm_dri.driver_id=dri_con.driver_id
    
    left join
    (select driver_id,
           min(dt) as driver_leave_date --司机离职日期，将该字段补上，然后做成增量,在dwm层，由于无法准确统计到司机离职日期，因此目前只是按照该逻辑粗略统计
    from oride_dw.dim_oride_driver_base
    where fault=6
    group by driver_id) dri_lea
    on dwm_dri.driver_id=dri_lea.driver_id;
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_driver_portrait_report_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_driver_portrait_report_task = PythonOperator(
    task_id='app_oride_driver_portrait_report_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_driver_base_df_prev_day_task >> app_oride_driver_portrait_report_task
dwm_oride_driver_finance_di_prev_day_task >> app_oride_driver_portrait_report_task
dwd_oride_order_base_include_test_di_prev_day_task >> app_oride_driver_portrait_report_task

dwd_oride_abnormal_order_di_prev_day_task >> app_oride_driver_portrait_report_task
dim_oride_driver_audit_base_prev_day_task >> app_oride_driver_portrait_report_task
dim_oride_driver_base_prev_day_task >> app_oride_driver_portrait_report_task

