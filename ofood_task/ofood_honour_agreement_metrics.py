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
                'ofood_dw_ods.ods_sqoop_base_jh_waimai_df',
                'ofood_dw_ods.ods_sqoop_base_jh_order_df',
                'ofood_dw_ods.ods_sqoop_base_jh_order_time_df',
                'ofood_dw_ods.ods_sqoop_base_jh_waimai_comment_df',
                'ofood_dw_ods.ods_sqoop_base_jh_waimai_order_df',
                'ofood_dw_ods.ods_sqoop_base_jh_order_log_df',
                'ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df',
                'ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df',
                'ofood_dw_ods.ods_sqoop_bd_invitation_info_df',
                'ofood_dw_ods.ods_sqoop_bd_jh_member_df',
                'ofood_dw_ods.ods_sqoop_base_jh_shop_account_df',
                'ofood_dw_ods.ods_sqoop_base_jh_waimai_product_df',

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
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_order_time_validate_task = HivePartitionSensor(
    task_id="jh_order_time_validate_task",
    table="ods_sqoop_base_jh_order_time_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_comment_validate_task = HivePartitionSensor(
    task_id="jh_waimai_comment_validate_task",
    table="ods_sqoop_base_jh_waimai_comment_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_order_log_validate_task = HivePartitionSensor(
    task_id="jh_order_log_validate_task",
    table="ods_sqoop_base_jh_order_log_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_order_validate_task = HivePartitionSensor(
    task_id="jh_waimai_order_validate_task",
    table="ods_sqoop_base_jh_waimai_order_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_shop_validate_task = HivePartitionSensor(
    task_id="jh_shop_validate_task",
    table="ods_sqoop_base_jh_shop_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_waimai_validate_task = HivePartitionSensor(
    task_id="jh_waimai_validate_task",
    table="ods_sqoop_base_jh_waimai_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_admin_users_validate_task = HivePartitionSensor(
    task_id="bd_admin_users_validate_task",
    table="ods_sqoop_bd_bd_admin_users_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_bd_fence_validate_task = HivePartitionSensor(
    task_id="bd_bd_fence_validate_task",
    table="ods_sqoop_bd_bd_bd_fence_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

bd_invitation_info_validate_task = HivePartitionSensor(
    task_id="bd_invitation_info_validate_task",
    table="ods_sqoop_bd_invitation_info_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

jh_member_validate_task = HivePartitionSensor(
    task_id="jh_member_validate_task",
    table="ods_sqoop_bd_jh_member_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


jh_shop_account_validate_task = HivePartitionSensor(
    task_id="jh_shop_account_validate_task",
    table="ods_sqoop_base_jh_shop_account_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


jh_waimai_product_validate_task = HivePartitionSensor(
    task_id="jh_waimai_product_validate_task",
    table="ods_sqoop_base_jh_waimai_product_df",
    partition="dt='{{ds}}'",
    schema="ofood_dw_ods",
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
        USING JAR 'oss://opay-datalake/udf-1.0-SNAPSHOT-jar-with-dependencies.jar';
        
        set hive.strict.checks.cartesian.product=false;
        set hive.mapred.mode=nonstrict;
        set hive.auto.convert.join = false;
        
        with 
        
        shop_dim as (
            select 
                city_id,
                shop_id,
                lat,
                lng
            from 
            ofood_dw_ods.ods_sqoop_base_jh_shop_df
            where dt = '${dt}'
        ),
        
        
        shop_metrics as (
            select  
            o.city_id as city_id,
            o.shop_id as shop_id,

            count(if(o.order_status in (-1,-2,-3) and o.pay_status <> 0 and o.refund_status <> 0,o.order_id,null)) as after_pay_cancel_order_cnt,
            count(if(o.order_status in (-1,-2,-3) and o.pay_status <> 0 and o.refund_status <> 0 and ol.cancel_from = 'system',o.order_id,null )) as sys_cancel_order_cnt,
            count(if(o.order_status in (-1,-2,-3) and o.pay_status <> 0 and o.refund_status <> 0 and ol.cancel_from = 'shop',o.order_id,null )) as merchant_cancel_order_cnt,
            count(if(o.order_status in (-1,-2,-3) and o.pay_status <> 0 and o.refund_status <> 0 and ol.cancel_from not in ( 'system','shop','admin'),o.order_id,null )) as user_cancel_order_cnt,
            avg((if(t.order_id is not null and t.pay_time > 0 and t.shop_jiedan_time > 0 ,t.shop_jiedan_time - t.pay_time,0)) /60) as accept_time_avg,
            avg((if(t.order_id is not null and t.order_compltet_time > 0 and t.shop_jiedan_time > 0,t.order_compltet_time - t.shop_jiedan_time,0)) / 60) as delivery_time_avg,
            avg(if(o.order_status = 8,w.score_peisong,null)) as score_peisong_avg
            
            
            from 
            (
                select 
                city_id,
                shop_id,
                order_id,
                order_status,
                pay_status,
                refund_status
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_df
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
                ofood_dw_ods.ods_sqoop_base_jh_order_time_df
                where dt = '${dt}'
                and from_unixtime(create_time,'yyyyMMdd') = '${ds}'
            ) t on o.order_id = t.order_id
            left join (
                select 
                order_id,
                score_peisong
                from 
                ofood_dw_ods.ods_sqoop_base_jh_waimai_comment_df
                where dt = '${dt}'
            ) w on o.order_id = w.order_id
            left join (
                select 
                order_id,
                \`from\` as cancel_from
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_log_df
                where dt = '${dt}'
                and status = -1 
                and from_unixtime(dateline,'yyyyMMdd') = '${ds}'
            ) ol on o.order_id = ol.order_id
            
            group by o.city_id ,
            o.shop_id
            
            
            
        ),
        
        
        shop_dim_metrics as (
        
            select 
            '${dt}' dt,
            sd.city_id,
            sd.shop_id,
            sd.lat,
            sd.lng,
            
            sm.after_pay_cancel_order_cnt,
            sm.sys_cancel_order_cnt,
            sm.merchant_cancel_order_cnt,
            sm.user_cancel_order_cnt,
            
            sm.accept_time_avg,
            sm.delivery_time_avg,
            sm.score_peisong_avg
            
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
        
                from ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df
                where dt = '${dt}' and job_id = 3 
            ) bdm
            left join (
                select 
                id,
                name
                from ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df
                where dt = '${dt}'
            ) hbdm on bdm.leader_id = hbdm.id
            left join (
                select 
                uid,
                area_name,
                points
                from ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df
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

        nvl(round(avg(s.accept_time_avg),1),0),
        nvl(round(avg(s.delivery_time_avg),1),0),
        nvl(round(avg(s.score_peisong_avg),1),0),
        
        nvl(sum(s.after_pay_cancel_order_cnt),0),
        nvl(sum(s.user_cancel_order_cnt),0),
        nvl(sum(s.merchant_cancel_order_cnt),0),
        nvl(sum(s.sys_cancel_order_cnt),0)
    
        from 
        bd_user_data u 
        left join 
        shop_dim_metrics s on u.dt = s.dt
        where isInArea(u.points,s.lat/1000000,s.lng/1000000) = 1
        group by u.area_name,u.points,u.id,u.bdm_name,u.hbdm_name
        ;
"
        echo ${bdm_dim_sql}
        beeline -u "jdbc:hive2://10.52.5.190:10000/default" -n airflow -e "${bdm_dim_sql}" 
    """,
    dag=dag,
)

create_shop_list_data = BashOperator(
    task_id='create_shop_list_data',
    bash_command="""
        dt="{{ ds }}"
        ds="{{ ds_nodash }}"

        shop_list_sql="

        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        SET mapreduce.job.queuename=root.airflow;
        
        create temporary function isInArea as 'com.oride.udf.IsInArea' 
        USING JAR 'oss://opay-datalake/udf-1.0-SNAPSHOT-jar-with-dependencies.jar';
        
        set hive.strict.checks.cartesian.product=false;
        set hive.mapred.mode=nonstrict;
        set hive.auto.convert.join = false;
        
        
        with shop_info as (
            select 
            '${dt}' dt,
            s.city_id,
            s.shop_id,
            s.title,
            s.lat,
            s.lng,
            nvl(w.closed,0) closed,
            nvl(w.addr,'') addr,
            nvl(w.yy_peitime,'') yy_peitime,
            nvl(a.account_number,'') account_number
        
            from 
            (
                select 
                city_id,
                shop_id,
                title,
                lat,
                lng
                from 
                ofood_dw_ods.ods_sqoop_base_jh_shop_df
                where dt = '${dt}'
            ) s 
            left join (
                select 
                shop_id,
                closed,
                addr,
                yy_peitime
                from 
                ofood_dw_ods.ods_sqoop_base_jh_waimai_df
                where dt = '${dt}'
            ) w on s.shop_id = w.shop_id
            left join (
                select 
                shop_id,
                account_number 
                from 
                ofood_dw_ods.ods_sqoop_base_jh_shop_account_df
                where dt = '${dt}'
            ) a on s.shop_id = a.shop_id
        ),
        
        shop_his_metrics as (
            select 
            o.shop_id,
            count(o.order_id) his_order_cnt,
            if(sum(first_shop + first_roof) > 0,1,0) is_new_user_act,
            if(sum(shop_amount + roof_amount) > 0,1,0) is_promotion_act
            from 
            (
                select 
                shop_id,
                order_id
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_df
                where dt = '${dt}'
            ) o 
            left join (
                select 
                order_id,
                shop_amount,
                roof_amount,
                first_shop,
                first_roof
                from 
                ofood_dw_ods.ods_sqoop_base_jh_waimai_order_df
                where dt = '${dt}'
            ) w on w.order_id = o.order_id
            group by o.shop_id 
        ),
        
        shop_product as (
            
            select 
            t.shop_id,
            count(t.product_id) product_cnt
            from 
            (
                select 
                product_id,
                shop_id,
                row_number() over(partition by product_id,shop_id order by dateline desc) order_by_id
                from ofood_dw_ods.ods_sqoop_base_jh_waimai_product_df
                where dt = '${dt}' and is_onsale = 1
            ) t 
            where t.order_by_id = 1
            group by t.shop_id
            
        ),
        
        
        bd_user_base as (
            select 
            id,
            name,
            leader_id,
            job_id
            from ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df
            where dt = '${dt}'
        ),
        
        
        bd_user as (
            select 
            '${dt}' dt,
            bd.id bd_id,
            bd.name bd_name,
            bdm.id bdm_id,
            bdm.name bdm_name,
            hbdm.id hbdm_id,
            hbdm.name hbdm_name,
            f.points
            from 
            (
                select 
                id,
                name,
                leader_id
                from 
                bd_user_base 
                where job_id = 4
            ) bd 
            left join 
            (
                select 
                id,
                name,
                leader_id
                from 
                bd_user_base 
                where job_id = 3
            ) bdm on bd.leader_id = bdm.id
            left join (
                select 
                id,
                name,
                leader_id
                from 
                bd_user_base 
                where job_id = 2
            ) hbdm on bdm.leader_id = hbdm.id
            left join (
                select 
                uid,
                points
        
                from 
                ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df
                where dt = '${dt}'
            ) f on f.uid = bd.id
        ),
        
        shop_sum_metrics as (
            select 
            si.dt,
            si.city_id,
            si.shop_id,
            si.title,
            si.lat,
            si.lng,
            si.closed,
            si.addr,
            si.yy_peitime,
            si.account_number,
            nvl(shm.his_order_cnt,0) his_order_cnt,
            nvl(shm.is_new_user_act,0) is_new_user_act,
            nvl(shm.is_promotion_act,0) is_promotion_act,
            nvl(sp.product_cnt,0) product_cnt
            from 
            shop_info si 
            left join shop_his_metrics shm on si.shop_id = shm.shop_id
            left join shop_product sp on sp.shop_id = si.shop_id
        ),
        
        in_bd_shop as (
            select 
            ssm.city_id,
            ssm.shop_id,
            ssm.title,
            ssm.lat,
            ssm.lng,
            ssm.closed,
            ssm.addr,
            ssm.yy_peitime,
            ssm.account_number,
            ssm.his_order_cnt,
            ssm.is_new_user_act,
            ssm.is_promotion_act,
            ssm.product_cnt,
            bu.bd_id,
            bu.bd_name,
            bu.bdm_id,
            bu.bdm_name,
            bu.hbdm_id,
            bu.hbdm_name
            
            from 
            shop_sum_metrics ssm 
            left join bd_user bu on bu.dt = ssm.dt
            where isInArea(bu.points,ssm.lat/1000000,ssm.lng/1000000) = 1
        ),
        
        
        out_bd_shop as (
            select 
            ssm.city_id,
            ssm.shop_id,
            ssm.title,
            ssm.lat,
            ssm.lng,
            ssm.closed,
            ssm.addr,
            ssm.yy_peitime,
            ssm.account_number,
            ssm.his_order_cnt,
            ssm.is_new_user_act,
            ssm.is_promotion_act,
            ssm.product_cnt,
            0 bd_id,
            '' bd_name,
            0 bdm_id,
            '' bdm_name,
            0 hbdm_id,
            '' hbdm_name
            from shop_sum_metrics ssm 
            left join in_bd_shop ibs on ssm.city_id = ibs.city_id and ssm.shop_id = ibs.shop_id
            where ibs.city_id is null or ibs.shop_id is null
        )
        
        
        insert overwrite table ofood_bi.ofood_shop_list_metrics_report partition(dt = '${dt}')
        select 
            city_id,
            shop_id,
            title,
            closed,
            addr,
            yy_peitime,
            account_number,
            his_order_cnt,
            is_new_user_act,
            is_promotion_act,
            product_cnt,
            bd_id,
            bd_name,
            bdm_id,
            bdm_name,
            hbdm_id,
            hbdm_name
        from 
            in_bd_shop
        union all
        select 
            city_id,
            shop_id,
            title,
            closed,
            addr,
            yy_peitime,
            account_number,
            his_order_cnt,
            is_new_user_act,
            is_promotion_act,
            product_cnt,
            bd_id,
            bd_name,
            bdm_id,
            bdm_name,
            hbdm_id,
            hbdm_name
        from 
            out_bd_shop
        
        ;
        
"
        echo ${shop_list_sql}
        beeline -u "jdbc:hive2://10.52.5.190:10000/default" -n airflow -e  "${shop_list_sql}" 
    """,
    dag=dag,
)



def send_shop_list_file_email(ds, ds_nodash, **kwargs):
    cursor = get_hive_cursor()
    sql = """
        select  
        dt,
        shop_id,
        title,
        bd_name,
        bdm_name,
        hbdm_name,
        his_order_cnt,
        if(closed = 0,'Y','N'),
        if(is_new_user_act = 1,'Y','N'),
        if(is_promotion_act = 1,'Y','N'),
        yy_peitime,
        product_cnt,
        addr,
        account_number
        
        from ofood_bi.ofood_shop_list_metrics_report 
        where dt = '{dt}' 

    """.format(dt=ds, ds=ds_nodash)

    headers = [
        'day',
        'shop_id',
        'title',
        'bd_name',
        'bdm_name',
        'hbdm_name',
        'his_order_cnt',
        'is_open(Y or N)',
        'activity_of_new_user(Y or N)',
        'activity_of_promotion(Y or N)',
        'business_time',
        'menu_item',
        'location',
        'opay_account'
    ]

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    file_name = '/tmp/ofood_shop_list_metrics_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)

    # send mail
    email_to = Variable.get("ofood_honour_metrics_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    email_subject = 'ofood-商家明细List每日数据_{dt}'.format(dt=ds)
    email_body = 'ofood-商家明细List每日数据'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


send_shop_list_file_email = PythonOperator(
    task_id='send_shop_list_file_email',
    python_callable=send_shop_list_file_email,
    provide_context=True,
    dag=dag
)


def send_bdm_dim_file_email(ds, ds_nodash, **kwargs):
    cursor = get_hive_cursor()
    sql = """
        select  
        dt,
        area_name,
        --points,
        bdm_name,
        hbdm_name,
        take_time_avg,
        delivery_time_avg,
        score_peisong_avg,
        cancel_order_cnt,
        concat(cast(nvl(round(sys_cancel_order_cnt * 100 / cancel_order_cnt,1),0) as string),'%'),
        concat(cast(nvl(round(user_cancel_order_cnt * 100/cancel_order_cnt,1),0) as string),'%'),
        concat(cast(nvl(round(merchant_cancel_order_cnt * 100/cancel_order_cnt,1),0) as string),'%')
        
        from ofood_bi.ofood_bdm_area_metrics_report 
        where dt = '{dt}' 

    """.format(dt=ds, ds=ds_nodash)

    headers = [
        'day',
        'area_name',
        #'points',
        'bdm_name',
        'hbdm_name',
        'time_pick',
        'time_peisong',
        'score_peisong',
        'total_cancle',
        'total_auto_cancle',
        'total_merchant_cancle',
        'total_user_cancle'
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


send_bdm_dim_file_email = PythonOperator(
    task_id='send_bdm_dim_file_email',
    python_callable=send_bdm_dim_file_email,
    provide_context=True,
    dag=dag
)

insert_bdm_dim_metrics = HiveToMySqlTransfer(
    task_id='insert_bdm_dim_metrics',
    sql=""" 
        
        select  
        null,
        dt,
        area_name,
        bdm_id, 
        bdm_name, 
        hbdm_name, 
        take_time_avg, 
        delivery_time_avg, 
        score_peisong_avg,
        cancel_order_cnt, 
        user_cancel_order_cnt, 
        merchant_cancel_order_cnt, 
        sys_cancel_order_cnt 
        
        from ofood_bi.ofood_bdm_area_metrics_report 
        where dt = '{{ ds }}'


        """,
    mysql_conn_id='mysql_bi',
    mysql_table='ofood_bdm_area_metrics_report',
    dag=dag)



insert_shop_list_metrics = HiveToMySqlTransfer(
    task_id='insert_shop_list_metrics',
    sql=""" 

        select  
        null,
        dt,
        bd_id,
        bdm_id,
        hbdm_id,
        city_id,
        shop_id,
        title,
        closed,
        addr,
        yy_peitime,
        account_number,
        his_order_cnt,
        is_new_user_act,
        is_promotion_act,
        product_cnt

        from ofood_bi.ofood_shop_list_metrics_report 
        where dt = '{{ ds }}'

        """,
    mysql_conn_id='mysql_bi',
    mysql_table='ofood_shop_list_metrics_report',
    dag=dag)

# bdm维度数据
validate_partition_data >> jh_waimai_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_waimai_order_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_shop_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_log_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_order_time_validate_task >> create_bdm_dim_data
validate_partition_data >> jh_waimai_comment_validate_task >> create_bdm_dim_data

validate_partition_data >> bd_admin_users_validate_task >> create_bdm_dim_data
validate_partition_data >> bd_bd_fence_validate_task >> create_bdm_dim_data
create_bdm_dim_data >> send_bdm_dim_file_email
create_bdm_dim_data >> insert_bdm_dim_metrics


# 商户明细list数据
validate_partition_data >> jh_shop_account_validate_task >> create_shop_list_data
validate_partition_data >> jh_waimai_product_validate_task >> create_shop_list_data
validate_partition_data >> bd_admin_users_validate_task >> create_shop_list_data
validate_partition_data >> bd_bd_fence_validate_task >> create_shop_list_data
validate_partition_data >> jh_order_validate_task >> create_shop_list_data
validate_partition_data >> jh_waimai_validate_task >> create_shop_list_data
validate_partition_data >> jh_waimai_order_validate_task >> create_shop_list_data
validate_partition_data >> jh_shop_validate_task >> create_shop_list_data



create_shop_list_data >> send_shop_list_file_email
create_shop_list_data >> insert_shop_list_metrics

