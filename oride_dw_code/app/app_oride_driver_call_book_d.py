# -*- coding: utf-8 -*-
"""
司机通讯录和通话次数
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor

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

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag
)


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

dwd_oride_driver_phone_list_mid_task = HiveOperator(
    task_id='dwd_oride_driver_phone_list_mid_task',
    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.dwd_oride_driver_phone_list_mid partition(country_code,dt)
            select a.user_id,e as name_phone_num,'nal' as country_code,'{pt}' as dt
            from oride_dw.dwd_oride_client_event_detail_hi a
            lateral view explode(split(substr(get_json_object(a.event_value,'$.phone_list'),2,length(get_json_object(a.event_value,'$.phone_list'))-2),',')) phone_list as e
            where a.dt between date_sub('{pt}',29) and '{pt}' and a.event_name='phone_list';
    '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}'
    ),
    schema='oride_dw',
    dag=dag
)

dwd_oride_driver_call_record_mid_task = HiveOperator(
    task_id='dwd_oride_driver_call_record_mid_task',
    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.dwd_oride_driver_call_record_mid  partition(country_code,dt)
            select b.user_id,f as name_phone_num,'nal' as country_code,'{pt}' as dt
            from oride_dw.dwd_oride_client_event_detail_hi b
            lateral view explode(split(substr(get_json_object(b.event_value,'$.call_record'),2,length(get_json_object(b.event_value,'$.call_record'))-2),',')) call_record as f
            where b.dt between date_sub('{pt}',29) and '{pt}' and b.event_name='call_record';    

    '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}'
    ),
    schema='oride_dw',
    dag=dag
)

app_oride_driver_call_book_d_task = HiveOperator(
    task_id='app_oride_driver_call_book_d_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.{table} partition(country_code,dt)
            select a3.user_id, --司机ID
                a3.contact_name, --联系人姓名
                a3.contact_phone_number, --联系人电话号吗
                if(b3.call_cnt is null,0,b3.call_cnt) as call_cnt,--与联系人通话次数
                'nal' AS country_code,--国家码
                '{pt}' as dt --日期
            from (
                select  a2.user_id,a2.contact_name,a2.contact_phone_number
                from(
                    select a1.user_id,
                    substr(split(a1.name_phone_num,'\":\"')[0],3) contact_name,
                    substr(split(a1.name_phone_num,'\":\"')[1],0,length(split(a1.name_phone_num,'\":\"')[1])-2) contact_phone_number 
                    from  oride_dw.dwd_oride_driver_phone_list_mid a1
                    where a1.dt='{pt}'
                )a2
                group by a2.user_id,a2.contact_name,a2.contact_phone_number
            ) a3
            left join
            (
                select b2.user_id,
                b2.contact_name,
                b2.contact_phone_number,
                count(1) call_cnt --通话次数
                --,b2.call_dur  --通话时长(总)
                --,max(last_call_time) as last_call_time --最后一次通话时间
                from(
                    select b1.user_id,
                    substr(split(b1.name_phone_num,'\":\"')[0],3)  contact_name,
                    substr(split(b1.name_phone_num,'\":\"')[1],0,length(split(b1.name_phone_num,'\":\"')[1])-2) contact_phone_number
                    --,split(a1.name_phone_num,'\":\"')[2] call_dur,--通话时长
                    --substr(split(a1.name_phone_num,'\":\"')[3],0,length(split(a1.name_phone_num,'\":\"')[3])-2) last_call_time --最后一次通话时间
                    from oride_dw.dwd_oride_driver_call_record_mid b1
                    where b1.dt='{pt}'
                ) as b2  
                group by b2.user_id,b2.contact_name,b2.contact_phone_number
            ) b3
            on a3.user_id=b3.user_id
            and a3.contact_name=b3.contact_name
            and a3.contact_phone_number=b3.contact_phone_number;
    '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)

# 生成_SUCCESS
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dwd_oride_client_event_detail_hi_task >> dwd_oride_driver_phone_list_mid_task >> sleep_time>> \
dwd_oride_driver_call_record_mid_task >> sleep_time>>app_oride_driver_call_book_d_task >> touchz_data_success