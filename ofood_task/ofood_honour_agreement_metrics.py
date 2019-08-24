# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.utils.email import send_email
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
import codecs
import csv
import logging

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 7, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'ofood_honour_agreement_metrics_daily',
    schedule_interval="10 04 * * *",
    default_args=args)

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names":
            [
                'ofood_dw.ods_sqoop_base_jh_waimai_df',
                'ofood_dw.ods_sqoop_base_jh_order_df',
                'ofood_dw.ods_sqoop_base_jh_order_time_df',
                'ofood_dw.ods_sqoop_base_jh_waimai_comment_df',
                'ofood_dw.ods_sqoop_base_jh_waimai_order_df',
                'ofood_dw.ods_sqoop_base_jh_order_log_df',
                'ofood_dw.ods_sqoop_bd_bd_admin_users_df',
                'ofood_dw.ods_sqoop_bd_bd_bd_fence_df',
                'ofood_dw.ods_sqoop_bd_invitation_info_df',
                'ofood_dw.ods_sqoop_bd_jh_member_df',

            ],
        # 任务名称
        "task_name": "ofood BD指标"
    },
    dag=dag
)

# 熔断阻塞流程
jh_order_validate_task = HivePartitionSensor(
    task_id="jh_order_validate_task",
    table="ods_sqoop_base_jh_order_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_order_time_validate_task = HivePartitionSensor(
    task_id="jh_order_time_validate_task",
    table="ods_sqoop_base_jh_order_time_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_comment_validate_task = HivePartitionSensor(
    task_id="jh_waimai_comment_validate_task",
    table="ods_sqoop_base_jh_waimai_comment_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_order_log_validate_task = HivePartitionSensor(
    task_id="jh_order_log_validate_task",
    table="ods_sqoop_base_jh_order_log_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_order_validate_task = HivePartitionSensor(
    task_id="jh_waimai_order_validate_task",
    table="ods_sqoop_base_jh_waimai_order_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_shop_validate_task = HivePartitionSensor(
    task_id="jh_shop_validate_task",
    table="ods_sqoop_base_jh_shop_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_validate_task = HivePartitionSensor(
    task_id="jh_waimai_validate_task",
    table="ods_sqoop_base_jh_waimai_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_admin_users_validate_task = HivePartitionSensor(
    task_id="bd_admin_users_validate_task",
    table="ods_sqoop_bd_bd_admin_users_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_bd_fence_validate_task = HivePartitionSensor(
    task_id="bd_bd_fence_validate_task",
    table="ods_sqoop_bd_bd_bd_fence_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_invitation_info_validate_task = HivePartitionSensor(
    task_id="bd_invitation_info_validate_task",
    table="ods_sqoop_bd_invitation_info_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_member_validate_task = HivePartitionSensor(
    task_id="jh_member_validate_task",
    table="ods_sqoop_bd_jh_member_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



create_bdm_dim_data = BashOperator(
    task_id='create_bdm_dim_data',
    bash_command="""
        dt="{{ ds }}"
        ds="{{ ds_nodash }}"

        bdm_dim_sql="
        
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
            
        create temporary function isInArea as 'com.oride.udf.IsInArea' 
                    USING JAR 'hdfs://node4.datalake.opay.com:8020/tmp/udf-1.0-SNAPSHOT-jar-with-dependencies.jar';
        
        with shop_dim as (
            select 
                city_id,
                shop_id,
                lat,
                lng
            from 
            ofood_dw.ods_sqoop_base_jh_shop_df 
            where dt = '${dt}'
        ),
        
        
        shop_metrics as (
            select  
            o.city_id as city_id,
            o.shop_id as shop_id,
            nvl(t.shop_jiedan_time ,0) as shop_jiedan_time,
            nvl(t.pay_time,0) as pay_time,
            nvl(t.order_compltet_time,0) as order_compltet_time,
            nvl(o.order_status,0) as order_status,
            nvl(w.score_peisong,0) as score_peisong,
            if(o.order_status = -2,1,0) as is_cancel,
            nvl(user_reason,0) as user_reason,
            nvl(merchant_reason,0) as merchant_reason
        
            from 
            (
                select 
                city_id,
                shop_id,
                order_id,
                order_status
                from 
                ofood_dw.ods_sqoop_base_jh_order_df
                where dt = '${dt}'
                and day = '${ds}'
            ) o 
            left join (
                select 
                order_id,
                pay_time,
                shop_jiedan_time,
                order_compltet_time
                from 
                ofood_dw.ods_sqoop_base_jh_order_time_df
                where dt = '${dt}'
                and from_unixtime(create_time,'yyyyMMdd') = '${ds}'
            ) t on o.order_id = t.order_id
            left join (
                select 
                order_id,
                score_peisong
                from 
                ofood_dw.ods_sqoop_base_jh_waimai_comment_df
                where dt = '${dt}'
            ) w on o.order_id = w.order_id
            left join (
                select 
                order_id,
                if(log like '%User cancelling order%'
                            or log like '%用户取消订单%',1,0) user_reason,
                if(log like '%Merchant cancelling order%',1,0) merchant_reason
                from 
                ofood_dw.ods_sqoop_base_jh_order_log_df
                where dt = '${dt}'
                and status = -1 
            ) ol on o.order_id = ol.order_id
        ),
        
        
        shop_dim_metrics as (
        
            select 
            '${dt}' dt,
            sd.city_id,
            sd.shop_id,
            sd.lat,
            sd.lng,
            sm.shop_jiedan_time,
            sm.pay_time,
            sm.order_compltet_time,
            sm.order_status,
            sm.score_peisong,
            sm.is_cancel,
            sm.user_reason,
            sm.merchant_reason
            
            from 
            shop_dim sd 
            left join 
            shop_metrics sm on sd.shop_id = sm.shop_id and sd.city_id = sm.city_id
        ),
        
        
        bd_user_data as (
        
            select 
            '${dt}' dt,
            b.area_name,
            b.points,
            bdm.id,
            bdm.name bdm_name,
            hbdm.name hbdm_name
            from 
            (
        
                select 
                id,
                name,
                leader_id
        
                from ofood_dw.ods_sqoop_bd_bd_admin_users_df 
                where dt = '${dt}' and job_id = 3 
            ) bdm
            left join (
                select 
                id,
                name
                from ofood_dw.ods_sqoop_bd_bd_admin_users_df 
                where dt = '${dt}'
            ) hbdm on bdm.leader_id = hbdm.id
            left join (
                select 
                uid,
                area_name,
                points
                from ofood_dw.ods_sqoop_bd_bd_bd_fence_df
                where dt = '${dt}'
            ) b on bdm.id = b.uid 
        )
        
        
        insert overwrite table ofood_bi.ofood_bdm_area_metrics_report partition(dt = '${dt}')
        select 
        u.area_name,
        u.points,
        u.id,
        u.bdm_name,
        u.hbdm_name,
        nvl(round(avg(if(s.shop_jiedan_time > 0 and s.pay_time > 0 ,s.shop_jiedan_time - s.pay_time,0))/60,1),0),
        nvl(round(avg(if(s.order_status = 8 and s.order_compltet_time > 0 and s.shop_jiedan_time > 0,s.order_compltet_time - s.shop_jiedan_time,0))/60,1),1),
        nvl(round(sum(if(s.order_status = 8 ,s.score_peisong,0))/sum(if(s.order_status = 8 and s.score_peisong > 0,1,0)),1),0),
        sum(if(is_cancel = 1,1,0)) ,
        sum(if(s.user_reason = 1,1,0)),
        sum(if(s.merchant_reason = 1,1,0))
        
        from 
        bd_user_data u 
        left join 
        shop_dim_metrics s on u.dt = s.dt
        where isInArea(u.points,s.lat/1000000,s.lng/1000000) = 1
        group by u.area_name,u.points,u.id,u.bdm_name,u.hbdm_name
        ;
"
        echo ${bdm_dim_sql}
        hive -e "${bdm_dim_sql}" 
    """,
    dag=dag,
)


def send_csv_file(ds, ds_nodash, **kwargs):
    cursor = get_hive_cursor()
    sql = """
        select  
        dt,
        area_name,
        points,
        bdm_name,
        hbdm_name,
        take_time_avg,
        delivery_time_avg,
        score_peisong_avg,
        cancel_order_cnt,
        cancel_order_cnt - user_cancel_order_cnt - merchant_cancel_order_cnt,
        user_cancel_order_cnt,
        merchant_cancel_order_cnt
        from ofood_bi.ofood_bdm_area_metrics_report 
        where dt = '{dt}' 

    """.format(dt=ds, ds=ds_nodash)

    headers = [
        'day',
        'area_name',
        'points',
        'bdm_name',
        'hbdm_name',
        'take_time_avg',
        'delivery_time_avg',
        'score_peisong_avg',
        'cancel_order_cnt',
        'sys_cancel_order_cnt',
        'user_cancel_order_cnt',
        'merchant_cancel_order_cnt'
    ]

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    file_name = '/tmp/ofood_bdm_dim_metrics_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)

    # send mail
    email_to = Variable.get("ofood_honour_metrics_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    email_subject = 'ofood-BDM履约每日数据_{dt}'.format(dt=ds)
    email_body = 'ofood-BDM履约每日数据'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

validate_partition_data >> jh_waimai_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_waimai_order_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_shop_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_log_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_time_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_waimai_comment_validate_task >> create_bdm_dim_data

validate_partition_data >> bd_admin_users_validate_task >> create_bdm_dim_data
validate_partition_data >> bd_bd_fence_validate_task >> create_bdm_dim_data


validate_partition_data >> bd_invitation_info_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_member_validate_task >> create_bdm_dim_data

create_bdm_dim_data >> send_file_email

