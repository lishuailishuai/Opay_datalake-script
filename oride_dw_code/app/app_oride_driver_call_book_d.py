# -*- coding: utf-8 -*-
"""
司机通讯录和通话次数（15天的通话记录）
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
import logging

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_call_book_d',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)


##----------------------------------------- 依赖 ---------------------------------------##

dwd_oride_client_event_detail_hi_task = HivePartitionSensor(
    task_id="dwd_oride_client_event_detail_hi_task",
    table="dwd_oride_client_event_detail_hi",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_call_book_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_driver_phone_list_mid_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.dwd_oride_driver_phone_list_mid partition(country_code,dt)
            select a.user_id,e as name_phone_num,'nal' as country_code,'{pt}' as dt
            from oride_dw.dwd_oride_client_event_detail_hi a
            lateral view explode(split(substr(get_json_object(a.event_value,'$.phone_list'),2,length(get_json_object(a.event_value,'$.phone_list'))-2),',')) phone_list as e
            where a.dt='{pt}' and a.event_name='phone_list';
    '''.format(
        pt=ds
    )
    return HQL

def dwd_oride_driver_call_record_mid_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.dwd_oride_driver_call_record_mid  partition(country_code,dt)
            select b.user_id,f as name_phone_num,'nal' as country_code,'{pt}' as dt
            from oride_dw.dwd_oride_client_event_detail_hi b
            lateral view explode(split(substr(get_json_object(b.event_value,'$.call_record'),2,length(get_json_object(b.event_value,'$.call_record'))-2),',')) call_record as f
            where b.dt='{pt}' and b.event_name='call_record';    

    '''.format(
        pt=ds
    )
    return HQL

def app_oride_driver_call_book_d_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table {db}.{table} partition(country_code,dt)

        SELECT user_id,
       --司机ID

       contact_name,
       --联系人姓名 （如果手机号没有在通讯录里，那么通话记录里的姓名应该是空，没有名字）

       contact_phone_number,
       --联系人电话

       sum(call_cnt) AS call_cnt,
       --与联系人通话次数

       'nal' AS country_code,
       --国家码

       '{pt}' AS dt --日期

FROM
  (SELECT a3.user_id,
          --司机ID

          (CASE WHEN a3.user_id=b3.user_id
           AND a3.contact_phone_number=b3.contact_phone_number THEN a3.contact_name ELSE NULL END) AS contact_name,
          --联系人姓名 （如果手机号没有在通讯录里，那么通话记录里的姓名应该是空，没有名字）

          a3.contact_phone_number,
          --联系人电话

          b3.call_cnt AS call_cnt --与联系人通话次数

   FROM
     (SELECT b2.user_id,
             b2.contact_name,
             b2.contact_phone_number,
             count(1) call_cnt --通话次数

      FROM
        (SELECT b1.user_id,
                substr(split(b1.name_phone_num,'\":\"')[0],3) AS contact_name,
                substr(split(b1.name_phone_num,'\":\"')[1],0,length(split(b1.name_phone_num,'\":\"')[1])-2) AS contact_phone_number
      FROM oride_dw.dwd_oride_driver_call_record_mid b1
      WHERE b1.dt='{pt}') AS b2
   GROUP BY b2.user_id,
            b2.contact_name,
            b2.contact_phone_number) b3 --通话记录

LEFT OUTER JOIN
  (SELECT a2.user_id,
          a2.contact_name,
          a2.contact_phone_number,
          length(a2.contact_name) contact_name_len,
          length(a2.contact_phone_number) contact_number_len
   FROM
     (SELECT a1.user_id,
             substr(split(a1.name_phone_num,'\":\"')[0],3) AS contact_name,
             substr(split(a1.name_phone_num,'\":\"')[1],0,length(split(a1.name_phone_num,'\":\"')[1])-2) AS contact_phone_number
   FROM oride_dw.dwd_oride_driver_phone_list_mid a1
   WHERE a1.dt='{pt}')a2
GROUP BY a2.user_id,
         a2.contact_name,
         a2.contact_phone_number ) a3 --手机通讯录
 ON a3.user_id=b3.user_id
AND a3.contact_phone_number=b3.contact_phone_number) x1
WHERE x1.dt BETWEEN date_sub('{pt}',14) AND '{pt}'
GROUP BY user_id,
         contact_name,
         contact_phone_number


    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


#主流程
def dwd_oride_driver_phone_list_mid(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_oride_driver_phone_list_mid_sql_task(ds)
    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


def dwd_oride_driver_call_record_mid(ds,**kargs):
    hive_hook=HiveCliHook()

    # 读取sql
    _sql = dwd_oride_driver_call_record_mid_sql_task(ds)
    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()
    # 读取sql
    _sql = app_oride_driver_call_book_d_sql_task(ds)

    logging.info('Executing: %s', _sql)
    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")

dwd_oride_driver_phone_list_mid_task = PythonOperator(
    task_id='dwd_oride_driver_phone_list_mid_task',
    python_callable=dwd_oride_driver_phone_list_mid,
    provide_context=True,
    dag=dag
)
dwd_oride_driver_call_record_mid_task = PythonOperator(
    task_id='dwd_oride_driver_call_record_mid_task',
    python_callable=dwd_oride_driver_call_record_mid,
    provide_context=True,
    dag=dag
)

app_oride_driver_call_book_d_task= PythonOperator(
    task_id='app_oride_driver_call_book_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_client_event_detail_hi_task>>dwd_oride_driver_phone_list_mid_task>>\
    dwd_oride_driver_call_record_mid_task>>app_oride_driver_call_book_d_task