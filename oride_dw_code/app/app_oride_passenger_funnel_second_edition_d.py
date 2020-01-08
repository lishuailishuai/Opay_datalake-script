# -*- coding: utf-8 -*-
"""
司机通话记录(二期)
"""
import airflow
from datetime import datetime, timedelta
from airflow.sensors import UFileSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveCliHook
import logging
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor
from airflow.models import Variable


args ={
    'owner':'chenghui',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_passenger_funnel_second_edition_d',
                  schedule_interval="40 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##
db_name="oride_dw"
table_name="app_oride_passenger_funnel_second_edition_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    dwm_oride_order_base_di_task = UFileSensor(
        task_id='dwm_oride_order_base_di_task',
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

    dim_oride_city_task = UFileSensor(
        task_id="dim_oride_city_task",
        filepath='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    dwd_oride_order_cancel_df_task = UFileSensor(
        task_id="dwd_oride_order_cancel_df_task",
        filepath='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_cancel_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
    dwm_oride_order_base_di_task = OssSensor(
        task_id='dwm_oride_order_base_di_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    dim_oride_city_task = OssSensor(
        task_id="dim_oride_city_task",
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    dwd_oride_order_cancel_df_task = OssSensor(
        task_id="dwd_oride_order_cancel_df_task",
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_cancel_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


#----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

def app_oride_passenger_funnel_second_edition_d_sql_task(ds):
    HQL='''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
          
    with city as(
        select *from oride_dw.dim_oride_city where dt='{pt}'
    )
    
    insert overwrite table {db}.{table} partition(country_code,dt)
    SELECT if(every_day.city_name is not null,every_day.city_name,lfw.city_name) as city_name,--城市ID
        if(every_day.product_id_every is not null,every_day.product_id_every,lfw.product_id_lfw) as product_id,--产品线ID
        if(every_day.has_cancel_reason_cnt is not null,every_day.has_cancel_reason_cnt,0) as has_cancel_reason_cnt, --有反馈取消单量
        if(every_day.cancel_cnt is not null,every_day.cancel_cnt,0)as cancel_cnt,--取消单量
        if(every_day.service_dur is NOT NULL, every_day.service_dur,0)as service_dur,--服务时长
        if(every_day.finish_order_cnt is NOT NULL, every_day.finish_order_cnt,0)as finish_order_cnt,--完单量
        if(every_day.bad_order_cnt is NOT NULL,every_day.bad_order_cnt,0)as bad_order_cnt,--差评单量
        if(every_day.evaluation_order_cnt is NOT NULL,every_day.evaluation_order_cnt,0)as evaluation_order_cnt,--评价单量
        if(lfw.submit_order_lfw_cnt is not NULL,lfw.submit_order_lfw_cnt,0)as submit_order_lfw_cnt,--下单量(四周均值)
        if(lfw.before_take_cancel_lfw_cnt is NOT NULL, lfw.before_take_cancel_lfw_cnt,0)as before_take_cancel_lfw_cnt,--应答前取消单量(四周均值)
        if(lfw.take_lfw_cnt is NOT NULL, lfw.take_lfw_cnt,0)as take_lfw_cnt,--应答单量(四周均值)
        if(lfw.after_take_cancel_lfw_cnt is NOT NULL,lfw.after_take_cancel_lfw_cnt,0)as after_take_cancel_lfw_cnt,--应答后取消单量(四周均值)
        if(lfw.driver_cancel_lfw_cnt is NOT NULL,lfw.driver_cancel_lfw_cnt,0)as driver_cancel_lfw_cnt,--司机取消单量(四周均值)
        if(lfw.completed_order_lfw_cnt is NOT NULL,lfw.completed_order_lfw_cnt,0) as completed_order_lfw_cnt, --完单量(四周均值)
        'nal' as country_code,
        '{pt}' as dt
    from(
        select city.city_name,
            every_in.product_id_every,
            cancel.has_cancel_reason_cnt,
            cancel.cancel_cnt,
            service_dur,
            finish_order_cnt,
            bad_order_cnt,
            evaluation_order_cnt
        from(
            SELECT city_id,product_id as product_id_every,
                sum(if(is_finish=1,order_service_dur,0)) as service_dur, --完单司机服务时长
                count(if(is_finish=1,order_id,null)) as finish_order_cnt, --完单量
                count(if(score<3,order_id,null)) as bad_order_cnt,--差评单量
                count(if(is_finish=1 and score IS NOT NULL,order_id,null)) evaluation_order_cnt --评价单量
            from oride_dw.dwm_oride_order_base_di 
            where dt='{pt}'
            GROUP BY city_id,product_id
        )as every_in
        left join
        (
            select city_id,
                product_id,
                count(id)as cancel_cnt,--取消单量
                sum(if(cancel_type>0 ,1,0))as has_cancel_reason_cnt--有反馈单数
            from  oride_dw.dwd_oride_order_cancel_df
            where dt='{pt}' 
                and cancel_role=1
                and from_unixtime(cancel_time,'yyyy-MM-dd')='{pt}'
                group by city_id,product_id
        )cancel
        on every_in.city_id=cancel.city_id and every_in.product_id_every=cancel.product_id
        left join city
        on every_in.city_id=city.city_id
    )as every_day
    FULL OUTER JOIN
    (   select city.city_name,
            lfw_in.product_id_lfw,
            submit_order_lfw_cnt,
            before_take_cancel_lfw_cnt,
            take_lfw_cnt,
            after_take_cancel_lfw_cnt,
            driver_cancel_lfw_cnt,
            completed_order_lfw_cnt
        from(
            SELECT city_id,product_id as product_id_lfw,
                count(order_id)/4 as submit_order_lfw_cnt, --下单量(四周均值)
                count(if(is_passanger_before_cancel=1,order_id,null))/4 as before_take_cancel_lfw_cnt,--应答前乘客取消单量(近四周均值)
                count(if(driver_id>0,order_id,null))/4 as take_lfw_cnt, --应答单量(近四周均值)
                count(if(is_after_cancel=1,order_id,null))/4 as after_take_cancel_lfw_cnt, --应答后取消单量(近四周均值)
                count(if(is_driver_after_cancel=1,order_id,null))/4 as driver_cancel_lfw_cnt, --司机取消单量(近四周均值)
                count(if(is_finish=1,order_id,null))/4 as completed_order_lfw_cnt --完单量(近四周均值)
                
            from oride_dw.dwm_oride_order_base_di
            WHERE datediff('{pt}',dt)>0 
                and datediff('{pt}',dt)<=28
                and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')
                    =from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'),'u')
            GROUP BY city_id,product_id
        )as lfw_in
        left join city
        on lfw_in.city_id=city.city_id
    )as lfw
    on every_day.city_name=lfw.city_name and every_day.product_id_every=lfw.product_id_lfw;
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
    _sql = app_oride_passenger_funnel_second_edition_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)
    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_passenger_funnel_second_edition_d_task = PythonOperator(
    task_id='app_oride_passenger_funnel_second_edition_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwm_oride_order_base_di_task>>app_oride_passenger_funnel_second_edition_d_task
dim_oride_city_task>>app_oride_passenger_funnel_second_edition_d_task
dwd_oride_order_cancel_df_task>>app_oride_passenger_funnel_second_edition_d_task