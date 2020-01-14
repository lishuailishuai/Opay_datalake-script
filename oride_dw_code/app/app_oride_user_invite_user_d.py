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
    'start_date': datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_user_invite_user_d',
                  schedule_interval="30 02 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_user_invite_user_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dwm_oride_passenger_base_df_prev_day_task = OssSensor(
        task_id='dwm_oride_passenger_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_base_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dwd_oride_coupon_use_detail_df_prev_day_task = OssSensor(
        task_id='dwd_oride_coupon_use_detail_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_coupon_use_detail_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

ods_sqoop_base_data_invite_df_prev_day_task = OssSensor(
        task_id='ods_sqoop_base_data_invite_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_invite",
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
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_user_invite_user_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(country_code,dt)
    select all_user.city_id,  --城市id
           concat_ws('-',substr(all_user.dt),weekofyear(all_user.dt)) as week,
           sum(if(uid.uid is not null,1,0)) as td_invite_user_cnt, --当天邀请人数
           sum(if(inved.invitee_id is not null,1,0)) as td_invited_user_cnt,  --当天被邀请人数
           sum(if(inved_all.invitee_id is not null and all_user.first_finish_ord_id is not null and all_user.first_finish_create_date=all_user.dt,1,0)) as fir_finish_invited_user_cnt, --截止目前所有被邀请人在当天完成首单的人数
           sum(if(uid_all.uid is not null,coupon_user.coupon_cnt,0)) as invite_user_coupon_cnt, --截止目前所有邀请人优惠券发放数量
           sum(if(inved_all.invitee_id is not null,all_user.acc_ord_cnt,0)) as invited_acc_ord_cnt, --截止目前所有被邀请人累计下单量
           sum(if(inved_all.invitee_id is not null,all_user.acc_finish_ord_cnt,0)) as invited_acc_finish_ord_cnt, --截止目前所有被邀请人累计完单量
           'nal' as country_code,
           '{pt}' as dt
    from (select * 
    from oride_dw.dwm_oride_passenger_base_df
    where dt='{pt}' and city_id<999001) all_user   --乘客全量信息表
    left join
    (select passenger_id,count(1) as coupon_cnt
    from oride_dw.dwd_oride_coupon_use_detail_df
    where dt='{pt}' 
    and receive_time>0
    and template_id=795
    group by passenger_id) coupon_user   --乘邀乘券表
    on all_user.passenger_id=coupon_user.passenger_id
    left join
    (select distinct uid
    from oride_dw_ods.ods_sqoop_base_data_invite_df 
    where dt='{pt}'
    and invitee_role=1 
    and role=1
    and from_unixtime((`timestamp`+1*60*60),'yyyy-MM-dd')='{pt}') uid   --邀请人
    on all_user.passenger_id=uid.uid
    left join
    (select distinct invitee_id
    from oride_dw_ods.ods_sqoop_base_data_invite_df 
    where dt='{pt}'
    and invitee_role=1 
    and role=1
    and from_unixtime((`timestamp`+1*60*60),'yyyy-MM-dd')='{pt}') inved   --被邀请人
    on all_user.passenger_id=inved.invitee_id
    left join
    (select distinct uid
    from oride_dw_ods.ods_sqoop_base_data_invite_df 
    where dt='{pt}'
    and invitee_role=1 
    and role=1) uid_all   --邀请人
    on all_user.passenger_id=uid_all.uid
    left join
    (select distinct invitee_id
    from oride_dw_ods.ods_sqoop_base_data_invite_df 
    where dt='{pt}'
    and invitee_role=1 
    and role=1) inved_all   --被邀请人
    on all_user.passenger_id=inved_his.invitee_id
    group by all_user.city_id;
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
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_user_invite_user_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_user_invite_user_d_task = PythonOperator(
    task_id='app_oride_user_invite_user_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_passenger_base_df_prev_day_task >> app_oride_user_invite_user_d_task
dwd_oride_coupon_use_detail_df_prev_day_task >> app_oride_user_invite_user_d_task
ods_sqoop_base_data_invite_df_prev_day_task >> app_oride_user_invite_user_d_task

