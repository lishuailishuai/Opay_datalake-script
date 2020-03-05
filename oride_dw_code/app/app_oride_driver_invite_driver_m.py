# -*- coding: utf-8 -*-
"""
司机邀请司机数据表
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.sensors import UFileSensor
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.models import Variable
from plugins.CountriesPublicFrame import CountriesPublicFrame

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_invite_driver_m',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_invite_driver_m"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

dim_oride_driver_audit_base_task = OssSensor(
        task_id='dim_oride_driver_audit_base_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_audit_base",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

ods_sqoop_base_data_invite_df_task = OssSensor(
        task_id='ods_sqoop_base_data_invite_df_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_invite",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dim_oride_driver_base_task = OssSensor(
        task_id='dim_oride_driver_base_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_base",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

dwm_oride_order_base_di_task = OssSensor(
        task_id='dwm_oride_order_base_di_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def app_oride_driver_invite_driver_m_sql_task(ds):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table {db}.{table} partition(country_code,dt)
        select nvl(dri.city_id,-10000) as city_id, --城市ID
               nvl(dri.product_id,-10000) as product_id,  --业务线
               sum(m.award) as award, --司邀司奖励
               sum(if(m.know_orider in(7,13,14),1,0)) as invited_driver_cnt, --当月被邀请司机数
               sum(if(m.know_orider in(7,13,14) and m.status=2,1,0)) as succ_invited_driver_cnt, --当月被成功邀请司机数
               sum(if(m.know_orider in(7,13,14) and m.status=2,nvl(ord.finish_ord_cnt,0),0)) as succ_invited_driver_finish_cnt,--当月被成功邀请司机完单数
               count(if(m.know_orider is not null,m.driver_id,null)) as settled_driver_cnt, --当月全渠道入驻司机数
               count(if(m.know_orider is not null and m.status=2,m.driver_id,null)) as succ_settled_driver_cnt, --当月全渠道成功入驻司机数
               sum(if(m.know_orider is not null and m.status=2,nvl(ord.finish_ord_cnt,0),0)) as succ_settled_driver_finish_cnt,  --当月全渠道成功入驻司机完单数
               nvl(dri.country_code,'nal') as country_code,
               '{pt}' as dt
        from 
        (select nvl(dri_recruit.driver_id,dri_invite.driver_id) as driver_id, --司机ID
               dri_recruit.know_orider as know_orider, --司机来源渠道
               dri_recruit.status,
               nvl(dri_recruit.create_time,dri_invite.invite_time) as invite_time, --邀请时间或者报名时间，看指标不同对应的时间含义不同
               nvl(dri_invite.award,0) as award
        from (select driver_id,
                nvl(know_orider,-999) as know_orider, --司机来源渠道
                status,
                from_unixtime(create_time,'yyyy-MM') as create_time  --司机报名时间
            from(select driver_id,
                   know_orider, --获取司机来源渠道
                   status,
                   create_time, --司机报名时间
                   row_number() over(partition by driver_id order by id desc) as rn
            from oride_dw.dim_oride_driver_audit_base
            where dt='{pt}' 
            and from_unixtime(create_time,'yyyy-MM')=substr(dt,1,7)
            ) t
            where t.rn=1) dri_recruit
        full outer join    
        (select uid as driver_id, --司机id
                from_unixtime(`timestamp`, 'yyyy-MM') as invite_time,  --邀请时间
                sum(award) as award  --给邀请人的奖励 
        from oride_dw_ods.ods_sqoop_base_data_invite_df
        where dt='{pt}'
              and from_unixtime(`timestamp`, 'yyyy-MM')=substr(dt,1,7)
              and valid=0
              and source in(7,13,14)
              and `role`=2 
              and invitee_role=2
              group by uid,
              from_unixtime(`timestamp`, 'yyyy-MM')) dri_invite
        on dri_recruit.driver_id=dri_invite.driver_id
        and dri_recruit.create_time=dri_invite.invite_time) m
        left join
        (select * 
        from oride_dw.dim_oride_driver_base
        where dt='{pt}') dri
        on m.driver_id=dri.driver_id
        left join
        (select substr(dt,1,7) as month,
                driver_id,
                sum(is_finish) as finish_ord_cnt --当天司机完单量
        from oride_dw.dwm_oride_order_base_di
        where substr(dt,1,7)=substr('{pt}',1,7)
        group by substr(dt,1,7),driver_id) ord
        on m.driver_id=ord.driver_id
        and m.invite_time=ord.month
        where dri.city_id<>999001
        group by nvl(dri.city_id,-10000),
                 nvl(dri.product_id,-10000),  --业务线 
                 nvl(dri.country_code,'nal');
    '''.format(
        pt=ds,
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
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_driver_invite_driver_m_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_driver_invite_driver_m_task = PythonOperator(
    task_id='app_oride_driver_invite_driver_m_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dim_oride_driver_audit_base_task >> app_oride_driver_invite_driver_m_task
ods_sqoop_base_data_invite_df_task >> app_oride_driver_invite_driver_m_task
dim_oride_driver_base_task >> app_oride_driver_invite_driver_m_task
dwm_oride_order_base_di_task >> app_oride_driver_invite_driver_m_task

