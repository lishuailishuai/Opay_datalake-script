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
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_abnormal_order_di',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_ods_sqoop_base_data_abnormal_order_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_abnormal_order_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_abnormal_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_abnormal_order_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_abnormal_order_di_task = HiveOperator(

    task_id='dwd_oride_abnormal_order_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    SELECT id,
       --业务自增id

       order_id,
       --涉及order_id

       driver_id,
       --司机id

       user_id,
       --用户id

       behavior_ids,
       --命中规则id

       rule_names,
       --命中规则名称

       is_revoked,
       --是否撤销，1是，0否

       create_time,
       --创建时间

       update_time,
       --更新时间

       score,
       --每单扣除分数

       amount,
       --扣款金额

       from_unixtime(create_time,'yyyy-MM-dd hh:mm:ss') AS f_create_time,
       --格式化创建时间(yyyy-MM-dd hh:mm:ss)

       from_unixtime(update_time,'yyyy-MM-dd hh:mm:ss') AS f_update_time,
       --格式化更新时间(yyyy-MM-dd hh:mm:ss)

       'nal' AS country_code,
       --国家码字段

       dt

        FROM oride_dw_ods.ods_sqoop_base_data_abnormal_order_df
        WHERE dt='{pt}'
        AND (from_unixtime(create_time,'yyyy-MM-dd')=dt
             OR from_unixtime(update_time,'yyyy-MM-dd')=dt) ;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
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
        $HADOOP_HOME/bin/hadoop fs -mkdir {hdfs_data_dir}
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
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

task_timeout_monitor
dependence_ods_sqoop_base_data_abnormal_order_df_prev_day_task >> \
sleep_time >> \
dwd_oride_abnormal_order_di_task >> \
touchz_data_success
