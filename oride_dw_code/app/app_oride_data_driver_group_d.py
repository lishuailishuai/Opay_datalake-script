# -*- coding: utf-8 -*-
"""
分中心提取数据
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors import UFileSensor
from datetime import datetime, timedelta
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

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

dag = airflow.DAG('app_oride_data_driver_group_d',
                  schedule_interval="00 04 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 60',
    dag=dag
)

##----------------------------------------- 依赖 ---------------------------------------##

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

dwd_oride_order_base_include_test_di_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

ods_sqoop_base_data_driver_group_df_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_group_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_group",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_data_driver_group_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

#----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "dt={pt}".format(pt=ds),"timeout": "1200"
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

app_oride_data_driver_group_d_task = HiveOperator(
    task_id='app_oride_data_driver_group_d_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_dw.{table} partition(dt)
    select k1.id,--小司管
        k1.driver_admin_name,--司管名字
        k1.group_type,--司管类型
        k1.product_id,--业务线:0全部1专车2快车3Otrike
        k1.driver_sign_num,--签约司机人数
        k1.driver_get_car_cnt,--领车数量
        round((k1.driver_reg_cnt-k1.finish_driver_cnt)/k1.driver_reg_cnt,2) driver_silence_percent,--沉默司机占比
        round(k1.driver_no_pay_num/k1.driver_reg_cnt,2) driver_no_pay_percent,--欠缴司机占比
        round(k1.finish_driver_cnt/k1.driver_reg_cnt,2) order_finished_rate,--完单存活司机率
        nvl(round(k1.finish_ord_cnt/k1.finish_driver_cnt,2),0) order_finished_avg_cnt,--人均完单量
        k1.driver_no_pay_num, --欠缴司机数量
        k1.dt
    from
    ( 
        select t2.id as id, --小司管
        t2.group_leader as driver_admin_name, --小司管名字
        '小司管' as group_type,--司管类型
        0 as product_id,--业务线:0全部1专车2快车3Otrike
        count(1) as driver_reg_cnt,--注册司机数,即存活司机数
        count(if(ord.driver_id is not null,t1.driver_id,null)) as finish_driver_cnt,  --完单司机数
        sum(ord.finish_ord_cnt) as finish_ord_cnt,  --完单量
        count(if(substr(t1.register_time,1,10)='{pt}',t1.driver_id,null)) as driver_sign_num, --签约司机人数
        count(if(t1.is_bind=1 and to_date(substr(t1.register_time,1,10))='{pt}',t1.driver_id,null)) driver_get_car_cnt,  --领车司机数
        count(if(t1.fault=5,t1.driver_id,null)) as driver_no_pay_num, --欠缴司机数量
        t1.dt

        from 
        (
            select register_time,driver_id,is_bind,fault,group_id,product_id,dt
            from oride_dw.dim_oride_driver_base 
            where dt='{pt}' and block=0
        ) t1
        left join
        (
            select a.driver_id,count(order_id) finish_ord_cnt
            from oride_dw.dwd_oride_order_base_include_test_di a		
            where dt='{pt}' and status in(4,5)
            group by a.driver_id
        ) ord
        on t1.driver_id=ord.driver_id
        LEFT JOIN 
        (
            select id,group_leader,--小司管名字
               group_leader_id,group_name--大司管名字
            from oride_dw_ods.ods_sqoop_base_data_driver_group_df 
            where dt='{pt}'
        )t2
        on t1.group_id=t2.id
        group by t2.id,t2.group_leader,t1.dt
    ) k1
    union
    select k2.id,--小司管
        k2.driver_admin_name,--小司管名字
        k2.group_type,--司管类型
        k2.product_id,--业务线:0全部1专车2快车3Otrike
        k2.driver_sign_num,--签约司机人数
        k2.driver_get_car_cnt,--领车数量
        round((k2.driver_reg_cnt-k2.finish_driver_cnt)/k2.driver_reg_cnt,2) driver_silence_percent,--沉默司机占比
        round(k2.driver_no_pay_num/k2.driver_reg_cnt,2) driver_no_pay_percent,--欠缴司机占比
        round(k2.finish_driver_cnt/k2.driver_reg_cnt,2) order_finished_rate,--完单存活司机率
        nvl(round(k2.finish_ord_cnt/k2.finish_driver_cnt,2),0) order_finished_avg_cnt,--人均完单量
        k2.driver_no_pay_num, --欠缴司机数量
        k2.dt
    from 
    ( 
        select  t2.id as id, --小司管
        t2.group_leader as driver_admin_name,--小司管名字
        '小司管' as group_type,--司管类型
        t1.product_id,--业务线:0全部1专车2快车3Otrike
        count(1) as driver_reg_cnt,--注册司机数,即存活司机数
        count(if(ord.driver_id is not null,t1.driver_id,null)) as finish_driver_cnt,  --完单司机数
        sum(ord.finish_ord_cnt) as finish_ord_cnt,  --完单量
        count(if(substr(t1.register_time,1,10)='{pt}',t1.driver_id,null)) as driver_sign_num, --签约司机人数
        count(if(t1.is_bind=1 and to_date(substr(t1.register_time,1,10))='{pt}',t1.driver_id,null)) driver_get_car_cnt,  --领车司机数
        count(if(t1.fault=5,t1.driver_id,null)) as driver_no_pay_num, --欠缴司机数量
        t1.dt
        from 
        (   
            select register_time,driver_id,is_bind,fault,group_id,product_id,dt
            from oride_dw.dim_oride_driver_base 
            where dt='{pt}' and block=0
        ) t1
        left join
        (
            select a.driver_id,count(order_id) finish_ord_cnt
            from oride_dw.dwd_oride_order_base_include_test_di a		
            where dt='{pt}' and status in(4,5)
            group by a.driver_id
        ) ord
        on t1.driver_id=ord.driver_id
        LEFT JOIN 
        (
            select id,group_leader,--小司管名字
                group_leader_id,group_name--大司管名字
            from oride_dw_ods.ods_sqoop_base_data_driver_group_df 
            where dt='{pt}'
        )t2
        on t1.group_id=t2.id
        group by t2.id,t2.group_leader,t1.dt,t1.product_id
    ) k2
    union
    select k3.group_leader_id as id,--大司管
        k3.driver_admin_name,--司管名字
        k3.group_type,--司管类型
        k3.product_id,--业务线:0全部1专车2快车3Otrike
        k3.driver_sign_num,--签约司机人数
        k3.driver_get_car_cnt,--领车数量
        round((k3.driver_reg_cnt-k3.finish_driver_cnt)/k3.driver_reg_cnt,2) driver_silence_percent,--沉默司机占比
        round(k3.driver_no_pay_num/k3.driver_reg_cnt,2) driver_no_pay_percent,--欠缴司机占比
        round(k3.finish_driver_cnt/k3.driver_reg_cnt,2) order_finished_rate,--完单存活司机率
        round(k3.finish_ord_cnt/k3.finish_driver_cnt,2) order_finished_avg_cnt,--人均完单量
        k3.driver_no_pay_num, --欠缴司机数量
        k3.dt
    from
    (
        select  t2.group_leader_id, --大司管
        t2.group_name as driver_admin_name,--大司管名字
        '大司管' as group_type,--司管类型
        0 as product_id,--业务线:0全部1专车2快车3Otrike
        count(1) as driver_reg_cnt,--注册司机数,即存活司机数
        count(if(ord.driver_id is not null,t1.driver_id,null)) as finish_driver_cnt,  --完单司机数
        sum(ord.finish_ord_cnt) as finish_ord_cnt,  --完单量
        count(if(substr(t1.register_time,1,10)='{pt}',t1.driver_id,null)) as driver_sign_num, --签约司机人数
        count(if(t1.is_bind=1 and to_date(substr(t1.register_time,1,10))='{pt}',t1.driver_id,null)) driver_get_car_cnt,  --领车司机数
        count(if(t1.fault=5,t1.driver_id,null)) as driver_no_pay_num, --欠缴司机数量
        t1.dt
        from 
        (
            select register_time,driver_id,is_bind,fault,group_id,product_id,dt
            from oride_dw.dim_oride_driver_base 
            where dt='{pt}' and block=0
        ) t1
        left join
        (
            select a.driver_id,count(order_id) finish_ord_cnt
            from oride_dw.dwd_oride_order_base_include_test_di a		
            where dt='{pt}' and status in(4,5)
            group by a.driver_id
        ) ord
        on t1.driver_id=ord.driver_id
        LEFT JOIN 
        (
            select id,group_leader,--小司管名字
                group_leader_id,if(group_leader_id=0,"未分配大司管",group_name) as group_name--大司管名字
            from oride_dw_ods.ods_sqoop_base_data_driver_group_df 
            where dt='{pt}'
        )t2
        on t1.group_id=t2.id
        group by t2.group_leader_id,t2.group_name,t1.dt
    ) k3
    union
    select k4.group_leader_id as id,--大司管
        k4.driver_admin_name,--司管名字
        k4.group_type,--司管类型
        k4.product_id,--业务线:0全部1专车2快车3Otrike
        k4.driver_sign_num,--签约司机人数
        k4.driver_get_car_cnt,--领车数量
        round((k4.driver_reg_cnt-k4.finish_driver_cnt)/k4.driver_reg_cnt,2) driver_silence_percent,--沉默司机占比
        round(k4.driver_no_pay_num/k4.driver_reg_cnt,2) driver_no_pay_percent,--欠缴司机占比
        round(k4.finish_driver_cnt/k4.driver_reg_cnt,2) order_finished_rate,--完单存活司机率
        round(k4.finish_ord_cnt/k4.finish_driver_cnt,2) order_finished_avg_cnt,--人均完单量
        k4.driver_no_pay_num, --欠缴司机数量
        k4.dt
    from
    (
        select  t2.group_leader_id, --大司管
        t2.group_name as driver_admin_name,--大司管名字
        '大司管' as group_type,--司管类型
        t1.product_id,--业务线:0全部1专车2快车3Otrike
        count(1) as driver_reg_cnt,--注册司机数,即存活司机数
        count(if(ord.driver_id is not null,t1.driver_id,null)) as finish_driver_cnt,  --完单司机数
        sum(ord.finish_ord_cnt) as finish_ord_cnt,  --完单量
        count(if(substr(t1.register_time,1,10)='{pt}',t1.driver_id,null)) as driver_sign_num, --签约司机人数
        count(if(t1.is_bind=1 and to_date(substr(t1.register_time,1,10))='{pt}',t1.driver_id,null)) driver_get_car_cnt,  --领车司机数
        count(if(t1.fault=5,t1.driver_id,null)) as driver_no_pay_num, --欠缴司机数量
        t1.dt
        from 
        (
            select register_time,driver_id,is_bind,fault,group_id,product_id,dt
            from oride_dw.dim_oride_driver_base 
            where dt='{pt}' and block=0
        ) t1
        left join
        (
            select a.driver_id,count(order_id) finish_ord_cnt
            from oride_dw.dwd_oride_order_base_include_test_di a		
            where dt='{pt}' and status in(4,5)
            group by a.driver_id
        ) ord
        on t1.driver_id=ord.driver_id
        LEFT JOIN 
        (
            select id,group_leader,--小司管名字
                group_leader_id,if(group_leader_id=0,"未分配大司管",group_name) as group_name--大司管名字
            from oride_dw_ods.ods_sqoop_base_data_driver_group_df 
            where dt='{pt}'
        )t2
        on t1.group_id=t2.id
        group by t2.group_leader_id,t2.group_name,t1.dt,t1.product_id
    ) k4;
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
        {
            "table": "{dag_name}".format(dag_name=dag_ids),
            "hdfs_path": "{hdfsPath}/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)
        }
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success=PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dim_oride_driver_base_task >> dwd_oride_order_base_include_test_di_task \
>> ods_sqoop_base_data_driver_group_df_task >> sleep_time \
>> app_oride_data_driver_group_d_task >> touchz_data_success