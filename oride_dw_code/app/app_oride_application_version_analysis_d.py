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
    'owner': 'lijialong',
    'start_date': datetime(2019, 10, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_application_version_analysis_d',
                  schedule_interval="40 03 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dim_oride_passenger_base_prev_day_task = UFileSensor(
    task_id='dim_oride_passenger_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_passenger_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_client_event_detail_hi_prev_day_task = UFileSensor(
    task_id='dwd_oride_client_event_detail_hi_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_application_version_analysis_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##


app_oride_application_version_analysis_d_task = HiveOperator(
    task_id='app_oride_application_version_analysis_d_task',
    hql='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;


    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)
        select 

        base_info.app_name,--app名称 oride为乘客端，oride driver 为司机端
        base_info.app_version,--版本号
        base_info.version_time, --版本时间
        base_info.platform, --操作系统
        
        count(distinct if(passenger_info.login_time is not null,passenger_info.passenger_id,null)) as active_passenger_cnt,--累积活跃乘客数
        count(distinct if(driver_info.login_time is not null,driver_info.driver_id,null)) as active_driver_cnt,--累积活跃司机数
        count(distinct if(passenger_info.login_time = '{pt}',passenger_info.passenger_id,null)) as yesterday_active_passenger_cnt,--昨日活跃乘客数、
        count(distinct if(driver_info.login_time = '{pt}',driver_info.driver_id,null)) as yesterday_active_driver_cnt,--昨日活跃司机数、
        count(distinct if(passenger_info.login_time = date_sub('{pt}',7),passenger_info.passenger_id,null)) as lastweek_active_passenger_cnt,--上周同期活跃乘客数、
        count(distinct if(driver_info.login_time = date_sub('{pt}',7),driver_info.driver_id,null)) as lastweek_active_driver_cnt,--上周同期活跃司机数、
    
        count(distinct if(passenger_info.login_time <= '{pt}' and passenger_info.login_time >= date_sub('{pt}',3),passenger_info.passenger_id,null)) as three_day_active_passenger_cnt,
        --近3日活跃乘客数
        count(distinct if(driver_info.login_time <= '{pt}' and driver_info.login_time >= date_sub('{pt}',3),driver_info.driver_id,null)) as three_day_active_driver_cnt,
        --近3日活跃司机数
        
        count(distinct if(passenger_info.login_time <= '{pt}' and passenger_info.login_time >= date_sub('{pt}',7),passenger_info.passenger_id,null)) as seven_day_active_passenger_cnt,
        --近7日活跃乘客数
        count(distinct if(driver_info.login_time <= '{pt}' and driver_info.login_time >= date_sub('{pt}',7),driver_info.driver_id,null)) as seven_day_active_driver_cnt,
        --近7日活跃司机数
        
        count(distinct if(passenger_info.login_time <= '{pt}' and passenger_info.login_time >= date_sub('{pt}',30),passenger_info.passenger_id,null)) as seven_day_active_passenger_cnt,
        --近30日活跃乘客数
        count(distinct if(driver_info.login_time <= '{pt}' and driver_info.login_time >= date_sub('{pt}',30),driver_info.driver_id,null)) as seven_day_active_driver_cnt,
        --近30日活跃司机数
        base_info.country_code,
        '{pt}'  as dt
    from
    (--app信息
        select
            t_base.app_name,
            t_base.app_version,
            t_base.platform,
            from_unixtime(cast(substr(t_base.event_time,1,10) as bigint),'yyyy-MM-dd HH:mm:ss') as version_time,--版本上线时间
            t_base.user_id,
            country_code,
            dt
        from
        (    
            select 
                app_name,--oride为乘客端，oride driver 为司机端
                app_version,--版本号
                platform, --操作系统
                user_id, --用户id
                min(if(event_name in('request_a_ride_click','accept_order_click') and event_time >='1569884400000' ,event_time,null)) over(partition by app_name,app_version,platform) as event_time,
                --min(if(event_time > '1546297200000' and length(event_time) >= 10 ,event_time,null)) over(partition by app_name,app_version,platform) as event_time,
                --2019-10-01 以后的数据不要  时间戳有负数， 0 、1 、2（但是这样的数据又不是脏数据）10位错误数据1230739718
                country_code,
                dt
            from oride_dw.dwd_oride_client_event_detail_hi base
            where event_name in ('oride_show','request_a_ride_click','accept_order_click')
                and app_name in ( 'oride','ORide Driver')
                and user_id != 0 
                and dt >= '2019-10-01'
            --union all
            --select 
            --    app_name,--oride为乘客端，oride driver 为司机端
            --    app_version,--版本号
            --    platform, --操作系统
            --    user_id, --用户id
            --    min(if(event_name in('accept_order_click') and event_time >='1569884400000' ,event_time,null)) over(partition by app_name,app_version,platform) as event_time,
                --min(if(event_time > '1546297200000' and length(event_time) >= 10 ,event_time,null)) over(partition by app_name,app_version,platform) as event_time,
                --2019-01-01 时间戳有负数， 0 、1 、2（但是这样的数据又不是脏数据）10位错误数据1230739718
            --    country_code,
            --    dt
            --from oride_dw.dwd_oride_client_event_detail_hi base
            --where event_name in ('oride_show','accept_order_click')
            --    and app_name = 'ORide Driver'
            --    and user_id != 0 
            --    and dt >= '2019-10-01'
            
        )t_base
        group by 
            t_base.app_name,
            t_base.app_version,
            t_base.platform,
            t_base.event_time,
            t_base.user_id,
            country_code,
            dt
    )base_info
    left join
    ( --乘客信息
        select
            'oride' as app_name,
            passenger_id,
            date_format(login_time,'yyyy-MM-dd') as login_time,
            dt
        from  oride_dw.dim_oride_passenger_base 
        where passenger_id != 0  and city_id != '999001' --测试数据。
        and dt >= '2019-10-01'
    )passenger_info on base_info.user_id = passenger_info.passenger_id and passenger_info.app_name = base_info.app_name and base_info.dt = passenger_info.dt
    left join
    (--司机信息
        select 
            'ORide Driver' as app_name,
            driver_id,
            date_format(login_time,'yyyy-MM-dd') as login_time,
            dt
        from oride_dw.dim_oride_driver_base
        where  driver_id != 0 and city_id != '999001' --测试数据，driver_id = 0 是北京的,过滤掉
            and dt >= '2019-10-01'
    )driver_info on base_info.user_id = driver_info.driver_id and driver_info.app_name = base_info.app_name and  base_info.dt = driver_info.dt
    where base_info.version_time is not null --过滤vesrion_time 为null值
    group by
            base_info.app_name, 
            base_info.app_version,
            base_info.version_time,
            base_info.platform,
            base_info.country_code;
'''.format(
        pt='{{ds}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)

# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dim_oride_driver_base_prev_day_task >> \
dependence_dim_oride_passenger_base_prev_day_task >> \
dependence_dwd_oride_client_event_detail_hi_prev_day_task >> \
sleep_time >> app_oride_application_version_analysis_d_task >> touchz_data_success
