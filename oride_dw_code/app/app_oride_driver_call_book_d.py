# -*- coding: utf-8 -*-
"""
司机通讯录和通话次数（15天的通话记录）
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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess


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
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  )


##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_call_book_d"

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

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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


def dwd_oride_driver_call_record_mid_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.dwd_oride_driver_call_record_mid partition(country_code,dt)
        select sum_record.user_id,
            sum_record.contact_name,
            sum_record.phone_num,
            '0' as end_time,
            count(1) as call_cnt,
            'nal' as country_code,
            '{pt}' as dt
        from(
            select b.user_id,
                substr(split(f,'\":\"')[0],3) as contact_name,
                substr(split(f,'\":\"')[1],0,length(split(f,'\":\"')[1])-2) as phone_num
            from oride_dw.dwd_oride_client_event_detail_hi b
            lateral view explode(split(substr(get_json_object(b.event_value,'$.call_record'),2,length(get_json_object(b.event_value,'$.call_record'))-2),',')) call_record as f
            where b.dt='{pt}' and b.event_name='call_record' and get_json_object(get_json_object(b.event_value,'$.call_record'),'$.call_time') is null
        )as sum_record 
        group by sum_record.user_id,sum_record.contact_name,sum_record.phone_num
        union
        select sum_new.user_id,
            sum_new.contact_name,
            sum_new.phone_num,
            max(sum_new.call_time) as end_time,--最后一次通话时间
            count(1) as call_cnt,
            'nal' as country_code,
            '{pt}' as dt
        from(
            select json_call.user_id,
                split(substr(split(json_call.call_info,',')[0],3,length(split(json_call.call_info,',')[0])-3),'\":\"')[0] as contact_name,
                split(substr(split(json_call.call_info,',')[0],3,length(split(json_call.call_info,',')[0])-3),'\":\"')[1] as phone_num,
                get_json_object(call_info,'$.call_time') as call_time
            from(
                select b.user_id,get_json_object(b.event_value,'$.call_record')as call_info
                from oride_dw.dwd_oride_client_event_detail_hi b
                where b.dt='{pt}' and b.event_name='call_record' 
                    and get_json_object(get_json_object(b.event_value,'$.call_record'),'$.call_time') is not null
                    and get_json_object(get_json_object(b.event_value,'$.call_record'),'$.call_duration')>0
                
            )json_call 
            where from_unixtime(cast(get_json_object(call_info,'$.call_time') as bigint),'yyyy-MM-dd')='{pt}'
        )as sum_new
        group by sum_new.user_id,sum_new.contact_name,sum_new.phone_num;
    '''.format(
        pt=ds
    )
    return HQL



def app_oride_driver_call_book_d_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table {db}.{table} partition(country_code,dt)

        SELECT base2.user_id,
            base2.contact_name,
            base2.phone_num,
            base2.end_time,
            base2.call_cnt,
            'nal' as country_code,
            '{pt}' as dt
        from(
            SELECT *,row_number() OVER(PARTITION BY user_id ORDER BY call_cnt DESC) as rn
            from(
                SELECT user_id, --司机ID
                    
                    contact_name, --联系人姓名
                    
                    phone_num, --联系人电话
                    max(end_time) as end_time, --最后一次通话时间
                    
                    sum(call_cnt) AS call_cnt --与联系人通话次数
                
                from oride_dw.dwd_oride_driver_call_record_mid
                WHERE dt BETWEEN date_sub('{pt}',14) AND '{pt}'
                GROUP BY user_id,contact_name,phone_num
            )as base
        )as base2 where base2.rn<=10;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


#主流程



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

dependence_dwd_oride_client_event_detail_hi>>dwd_oride_driver_call_record_mid_task>>app_oride_driver_call_book_d_task