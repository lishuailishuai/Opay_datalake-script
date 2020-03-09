# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
    'owner': 'lishuai',
    'start_date': datetime(2020, 3, 5),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_score_activity_conf_df',
                  schedule_interval="00 02 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_data_driver_score_activity_conf_df_task = OssSensor(
    task_id='ods_sqoop_base_data_driver_score_activity_conf_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_score_activity_conf",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_driver_score_activity_conf_df"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_driver_score_activity_conf_df_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

          select
          id,--'无业务含义主键'
          serv_type,--'服务类型'
          country_id,--'国家ID'
          city_id,--城市ID
          status,--状态 0:废弃 1:开启
          aa.enable,--每日首单积分状态
          aa.extra_score_for_new_pax,--每日首单额外积分
          get_json_object(aa.first_order_score,'$.1'),--每日首单首日积分
          get_json_object(aa.first_order_score,'$.2'),--每日首单2日积分
          get_json_object(aa.first_order_score,'$.3'),--每日首单3日积分
          get_json_object(aa.first_order_score,'$.4'),--每日首单4日积分
          get_json_object(aa.first_order_score,'$.5'),--每日首单5日积分
          get_json_object(aa.first_order_score,'$.6'),--每日首单6日积分
          get_json_object(aa.first_order_score,'$.7'),--每日首单7日积分
          cc.enable,--差单状态
          cc.pick_dist_limit,--差单得分的接驾距离最低限制
          cc.score,--接驾距离超过最低限制的差单所得积分
          dd.enable,--完单积分状态
          
          ff.start_time,--高峰期完单的开始时间
          ff.end_time,--高峰期完单的结束时间
          ff.score,--高峰期完单，每单得的积分
          
          gg.score,--非高峰期完单，每单得的积分
          hh.enable,--骑手有责拒单状态
          hh.neg_score,--骑手拒单，每单减的积分
          from_unixtime(created_at),--创建时间
          from_unixtime(updated_at),--更新时间
          tag,--骑手标签
          grab_score,--枪单积分
          'nal' as country_code,
          '{pt}' as dt
          from
          oride_dw_ods.ods_sqoop_base_data_driver_score_activity_conf_df
          LATERAL VIEW EXPLODE(from_json(get_json_object(finish_order_score,'$.rush_hour'),array(named_struct("start_time","","end_time","","score","")))) es AS ff
          
          lateral view explode(split(regexp_extract(city_ids,'^\\[(.+)\\]$',1),',')) city as city_id
          lateral view json_tuple(first_order_score,'enable','extra_score_for_new_pax','first_order_score') aa as enable,extra_score_for_new_pax,first_order_score
          --lateral view json_tuple(first_order_score,'1','2','3','4','5','6','7') bb as one,two,three,four,five,six,seven
          lateral view json_tuple(low_value_order_score,'enable','pick_dist_limit','score') cc as enable,pick_dist_limit,score
          lateral view json_tuple(finish_order_score,'enable','rush_hour','non_rush_hour') dd as enable,rush_hour,non_rush_hour
          
          --LATERAL VIEW EXPLODE(from_json(get_json_object(finish_order_score,'$.rush_hour'),array(named_struct("start_time","","end_time","","score","")))) es AS ff
          
          lateral view json_tuple(non_rush_hour,'score') gg as score
          lateral view json_tuple(driver_duty_cancel_score,'enable','neg_score') hh as enable,neg_score
          where dt='{pt}'
          
        
          
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

##  使用python字符串的format方法时，大括号是特殊转义字符，如果需要原始的大括号，用{{代替{, 用}}代替}  否则会报单个}的错误哦
##  这里只能需要一个  用}}代替}，否则其他都报错


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
        第一个参数true: 数据目录是有country_code分区。false 没有
        第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql_task(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_driver_score_activity_conf_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwd_oride_driver_score_activity_conf_df_task = PythonOperator(
    task_id='dwd_oride_driver_score_activity_conf_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_sqoop_base_data_driver_score_activity_conf_df_task >> dwd_oride_driver_score_activity_conf_df_task