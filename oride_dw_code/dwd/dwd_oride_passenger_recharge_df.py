# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor

from utils.connection_helper import get_hive_cursor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_passenger_recharge_df',
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
ods_sqoop_base_data_user_recharge_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_user_recharge_df_prev_day_task",
    table="ods_sqoop_base_data_user_recharge_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_passenger_recharge_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_passenger_recharge_df_task = HiveOperator(
    task_id='dwd_oride_passenger_recharge_df_task',

    hql='''
SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select
from_unixtime(create_time,'yyyy-MM-dd') as create_date, --充值日期
user_id, -- 用户 ID
count(id) as user_recharge_cnt, --充值次数
count(if(status = 1,id,null)) as user_recharge_succ_cnt,  -- 充值成功次数
sum(if(status= 1,balance,0.0)) as user_recharge_succ_balance, --充值成功真实金额
sum(if(status = 1,bonus,0.0)) as user_recharge_succ_bonus, --充值成功奖励金
sum(if(status = 1,amount,0.0)) as user_recharge_succ_amount,  --账户总额
'nal' as country_code,
'{pt}' as dt
from oride_dw_ods.ods_sqoop_base_data_user_recharge_df
where dt='{pt}'
group by from_unixtime(create_time,'yyyy-MM-dd'),
user_id;
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)


def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT user_id,create_date,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY user_id,create_date HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
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


ods_sqoop_base_data_user_recharge_df_prev_day_task >>\
sleep_time >> \
dwd_oride_passenger_recharge_df_task >> \
task_check_key_data >> \
touchz_data_success
