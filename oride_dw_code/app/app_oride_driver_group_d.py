# -*- coding: utf-8 -*-
"""
分中心提取数据（大司管）
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors import UFileSensor
from datetime import datetime, timedelta
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from utils.connection_helper import get_hive_cursor
from airflow.sensors.s3_key_sensor import S3KeySensor
import logging
from airflow.models import Variable
from airflow.sensors import OssSensor



args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 10, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_group_d',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_group_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    dim_oride_driver_base_task = UFileSensor(
        task_id='dim_oride_driver_base_task',
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str='oride/oride_dw/dim_oride_driver_base',
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dwd_oride_order_base_include_test_di_task = S3KeySensor(
        task_id='dwd_oride_order_base_include_test_di_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-bi',
        poke_interval=60,
        dag=dag
    )

    dwd_oride_driver_data_group_df_task = UFileSensor(
        task_id='dwd_oride_driver_data_group_df_task',
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_data_group_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
#路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    dim_oride_driver_base_task = OssSensor(
        task_id='dim_oride_driver_base_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str='oride/oride_dw/dim_oride_driver_base',
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dwd_oride_order_base_include_test_di_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dwd_oride_driver_data_group_df_task = OssSensor(
        task_id='dwd_oride_driver_data_group_df_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_data_group_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

#----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds),"timeout": "1200"
        }
    ]
    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor =  PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_group_d_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table {db}.{table} partition(country_code,dt)
        select k1.group_name, --司管名字
            k1.driver_sign_num,--签约司机人数
            k1.driver_get_car_cnt,--领车数量
            round((k1.driver_reg_cnt-k1.finish_driver_cnt)/k1.driver_reg_cnt,2) driver_silence_percent,--沉默司机占比
            round(k1.driver_no_pay_num/k1.driver_reg_cnt,4) driver_no_pay_percent,--欠缴司机占比
            round(k1.finish_driver_cnt/k1.driver_reg_cnt,4) order_finished_rate,--完单存活司机率
            nvl(round(k1.finish_ord_cnt/k1.finish_driver_cnt,2),0) order_finished_avg_cnt,--人均完单量
            k1.driver_no_pay_num, --欠缴司机数量
            'nal' AS country_code,--国家码
            '{pt}' dt --日期
        from
        (
            select t.group_name, --大司管名字
                count(distinct t.driver_id) as driver_reg_cnt,
                if(sum(t.finish_ord_cnt) is not null,sum(t.finish_ord_cnt),0) finish_ord_cnt,  --完单量
                count(if(t.driver_id_order is not null,t.driver_id,null)) finish_driver_cnt,--完单司机量
                count(if(substr(t.register_time,1,10)='{pt}',t.driver_id,null)) driver_sign_num, --签约司机人数
                count(if(t.is_bind=1 and substr(t.register_time,1,10)='{pt}',t.driver_id,null)) driver_get_car_cnt,  --领车司机数
                count(if(t.fault=5,t.driver_id,null)) as driver_no_pay_num --欠缴司机量
            from
            (  select k.driver_id,
                    k.driver_name,
                    k.group_id,
                    k.fault,
                    k.register_time,
                    k.is_bind,
                    k.id,
                    k.group_leader,
                    k.group_leader_id,
                    k.driver_id_order,
                    k.finish_ord_cnt,
                    if(regexp_extract(k.group_name,'(.*?).[0-9]',1)='',k.group_name,regexp_extract(k.group_name,'(.*?).[0-9]',1)) as group_name 
                from
                ( 
                    select a.driver_id,a.driver_name,a.group_id,a.fault,a.register_time,a.is_bind,
                        b.id,b.group_leader,b.group_name,b.group_leader_id,
                        c.driver_id as driver_id_order,c.finish_ord_cnt
                    from 
                    (
                        select driver_id,driver_name,group_id,block,fault,dt,register_time,is_bind,product_id
                        from oride_dw.dim_oride_driver_base 
                        where dt='{pt}' and city_name='Lagos' --and block=0 
                        and product_id=1
                    ) a
                    left join 
                    (
                        select id,group_leader,group_name,group_leader_id 
                        from oride_dw.dwd_oride_driver_data_group_df 
                        where dt='{pt}'
                    ) b
                    on a.group_id=b.id
                    left join 
                    (
                        select driver_id,count(order_id) finish_ord_cnt
                        from oride_dw.dwd_oride_order_base_include_test_di
                        where dt='{pt}' and status in(4,5)
                        group by driver_id
                    ) c
                    on a.driver_id=c.driver_id
                    where a.group_id!=0
                ) k
            )t
            group by t.group_name
        ) k1;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


#主流程
def execution_data_task_id(ds,**kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_driver_group_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true")

app_oride_driver_group_d_task = PythonOperator(
    task_id='app_oride_driver_group_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_oride_driver_base_task >>app_oride_driver_group_d_task

dwd_oride_order_base_include_test_di_task >>app_oride_driver_group_d_task

dwd_oride_driver_data_group_df_task >> app_oride_driver_group_d_task