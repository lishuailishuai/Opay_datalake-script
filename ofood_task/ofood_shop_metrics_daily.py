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
    'ofood_shop_metrics_daily',
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



insert_shop_metrics = HiveOperator(
    task_id='insert_shop_metrics',
    hql=''' 
        with 
        order_data as (
            select
            from_unixtime(unix_timestamp('{{ ds_nodash }}', 'yyyyMMdd'),'yyyyMMdd') day,
            s.title,
            s.shop_id,
            s.lat,
            s.lng,
            nvl(count(o.order_id),0) order_num,
            nvl(count(if(o.order_status = 8 and o.shop_id is not null,o.order_id,null)),0) success_order_num,
            nvl(count(if(o.pay_status = 1 and o.shop_id is not null,o.order_id,null)),0) pay_order_num,
            nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null,d.origin_product + d.origin_package + d.origin_delivery,0)),0) trade_price_sum,
            nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null,d.origin_product + d.origin_package + d.origin_delivery - o.order_youhui - o.first_youhui,0)),0) actual_trade_price_sum,
            nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null, d.first_roof + d.roof_mj + d.roof_delivery + d.roof_capped ,0)),0) cost_price_sum,
            nvl(sum(if(o.order_status = 8 and o.shop_id is not null and d.order_id is not null,d.first_shop + d.shop_mj + d.shop_delivery + d.shop_capped,0)),0) amount_and_first_price_sum,
            nvl(sum(if(o.order_status = 8 and o.shop_id is not null,o.amount,0)),0) amount_price_sum,
            nvl(count(if(o.order_status = 8 and t.order_id is not null and  (t.order_compltet_time - o.pay_time)/60 <= 40,o.order_id,null)),0) in_time_order_num,
            nvl(count(if(o.order_status = 8 and t.order_id is not null and w.order_id is not null 
            and  (t.order_compltet_time - o.pay_time)/60 <= 40 and w.score_peisong <= 2,o.order_id,null)),0) in_time_negative_order_num,
            --nvl(count(if(o.order_status = -1 and o.pay_status = 0,o.order_id,null)),0) cancel_num,
            nvl(count(if(o.pay_status = 0 and o.order_status in (-1,-2,-3),o.order_id,null)),0) before_pay_cancel_num,
            nvl(count(if(o.pay_status <> 0 and o.refund_status <> 0 and o.order_status in (-1,-2,-3),o.order_id,null)),0) after_pay_cancel_num,
            nvl(count(if(o.pay_status <> 0 and o.refund_status <> 0 and o.order_status in (-1,-2,-3) and lu.order_id is null,o.order_id,null)),0) user_cancel_num,
            nvl(count(if(o.pay_status <> 0 and o.refund_status <> 0 and o.order_status in (-1,-2,-3) and lm.order_id is not null,o.order_id,null)),0) merchant_cancel_num,
            nvl(count(if(o.pay_status <> 0 and o.refund_status <> 0 and o.order_status in (-1,-2,-3) and ls.order_id is not null,o.order_id,null)),0) as sys_cancel_num,
            1 is_open
            from 
            (
                select 
                shop_id,
                title,
                lat,
                lng
                from 
                ofood_dw_ods.ods_sqoop_base_jh_waimai_df
                where dt = '{{ ds }}'  
                and closed=0 and audit=1
            ) s
            
            left join ofood_dw_ods.ods_sqoop_base_jh_order_df o on o.dt = '{{ ds }}' and o.day = '{{ ds_nodash }}' and o.shop_id = s.shop_id
            left join ofood_dw_ods.ods_sqoop_base_jh_order_time_df t on t.dt = '{{ ds }}' and  o.order_id = t.order_id
            left join ofood_dw_ods.ods_sqoop_base_jh_waimai_comment_df w on w.dt = '{{ ds }}' and  o.order_id = w.order_id
            left join ofood_dw_ods.ods_sqoop_base_jh_waimai_order_df d on d.dt = '{{ ds }}' and o.order_id = d.order_id
            left join (
                select 
                order_id
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_log_df
                where dt = '{{ ds }}' and status = -1 
                and `from` in('system','shop','admin') 
                and from_unixtime(dateline,'yyyyMMdd') = '{{ ds_nodash }}'
            ) lu on o.order_id = lu.order_id
            left join ( 
                select 
                order_id
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_log_df
                where dt = '{{ ds }}' and status = -1 
                and `from`='shop'
                and from_unixtime(dateline,'yyyyMMdd') = '{{ ds_nodash }}'
            ) lm on o.order_id = lm.order_id
            left join ( 
                select 
                order_id
                from 
                ofood_dw_ods.ods_sqoop_base_jh_order_log_df
                where dt = '{{ ds }}' and status = -1 
                and `from`='system'
                and from_unixtime(dateline,'yyyyMMdd') = '{{ ds_nodash }}'
            ) ls on o.order_id = ls.order_id
            group by from_unixtime(unix_timestamp('{{ ds_nodash }}', 'yyyyMMdd'),'yyyyMMdd'),s.title,s.shop_id,s.lat,s.lng
        ),
        
        
        
        new_data as 
        (
        
            select 
            from_unixtime(unix_timestamp('{{ ds_nodash }}', 'yyyyMMdd'),'yyyyMMdd') day,
            s.shop_id,
            nvl(count(distinct if(s.ft = '{{ ds_nodash }}',s.uid,null)),0) new_user_place_num,
            nvl(count(distinct if(s.ft = '{{ ds_nodash }}' and s.is_invitation > 0,s.uid,null)),0) new_user_place_invitation_num,
            nvl(count(distinct if(s.ft = '{{ ds_nodash }}' and s.order_status = 8,s.uid,null)),0) new_user_complete_num,
            nvl(count(distinct if(s.ft = '{{ ds_nodash }}' and s.order_status = 8 and s.is_invitation > 0,s.uid,null)),0) new_user_complete_invitation_num
            from 
            (
                select 
                t.shop_id,
                t.uid,
                t.order_status,
                t.is_invitation,
                t.ft,
                row_number() over(partition by t.shop_id,t.uid,t.order_status order by t.ft) order_by
                from 
                (
                    select 
                    o.shop_id,
                    o.order_status,
                    o.uid,
                    count(distinct(i.uid)) is_invitation,
                    from_unixtime(min(dateline),'yyyyMMdd') ft
                    from 
                    (
                        select 
                        t.shop_id,
                        t.order_status,
                        t.dateline,
                        t.uid
                        from 
                        (
                            select 
                            o.shop_id,
                            o.order_status,
                            o.dateline,
                            o.uid,
                            row_number() over(partition by o.uid,o.order_status order by o.dateline) order_by
                            from 
                            ofood_dw_ods.ods_sqoop_base_jh_order_df o
                            where o.dt = '{{ ds }}'
                        ) t 
                        where t.order_by = 1
                    ) o 
                    left join (
                        select 
                        uid
                        from 
                        ofood_dw_ods.ods_sqoop_bd_invitation_info_df
                        where dt = '{{ ds }}'
                    ) i on i.uid = o.uid 
                    
                    group by o.shop_id,o.uid,o.order_status
                ) t
            ) s
            where s.order_by = 1 
            group by s.shop_id
        
        ),
        
        
        new_data_merchant as 
        (   
        
            select 
            from_unixtime(unix_timestamp('{{ ds_nodash }}', 'yyyyMMdd'),'yyyyMMdd') day,
            r.shop_id,
            min(r.ft) first_place_date,
            min(if(r.order_status = 8,r.ft,'99999999')) first_complete_date
            
            from 
            (
                select 
                
                s.shop_id,
                s.order_status,
                s.ft
                from 
                (
                select 
                t.shop_id,
                t.order_status,
                t.ft,
                row_number() over(partition by t.shop_id,t.order_status order by t.ft) order_id
                from 
                    (
                        select 
                        o.shop_id,
                        o.order_status,
                        from_unixtime(min(dateline),'yyyyMMdd') ft
                        from 
                        ofood_dw_ods.ods_sqoop_base_jh_order_df o
                        where o.dt = '{{ ds }}'
                        group by o.shop_id,o.order_status
                    ) t
                ) s
                where s.order_id = 1
            ) r 
            group by r.shop_id
        )
        
        
        
        insert overwrite table ofood_bi.ofood_order_shop_metrics_report partition (dt = '{{ ds }}')
        select 
        od.shop_id shop_id,
        od.title title,
        od.lat lat,
        od.lng lng,
        od.success_order_num number_of_valid_order,
        od.pay_order_num pay_order_num,
        od.cost_price_sum,
        od.amount_and_first_price_sum,
        od.amount_price_sum,
        od.trade_price_sum gmv,
        od.actual_trade_price_sum net_turnover,
        nvl(nd.new_user_place_num,0) total_number_of_new_users,
        nvl(nd.new_user_complete_num,0) number_of_new_users,
        od.in_time_order_num punctual_arrival_number_of_order,
        od.in_time_negative_order_num negative_comment_number_of_order,
        od.order_num total_number_of_order,
        --od.cancel_num number_of_cancel_order_before_payment,
        0 as number_of_cancel_order_before_payment , --字段已弃用，占位使用
        od.sys_cancel_num number_of_cancel_order_auto,
        od.user_cancel_num number_of_cancel_order_user,
        od.merchant_cancel_num number_of_cancel_order_shop,
        od.before_pay_cancel_num,
        od.after_pay_cancel_num,
        od.is_open,
        if(ndm.shop_id is not null and ndm.first_place_date = '{{ ds_nodash }}',1,0) is_first_placed_order,
        if(ndm.shop_id is not null and ndm.first_complete_date = '{{ ds_nodash }}',1,0) is_first_completed_order,
        nvl(nd.new_user_place_invitation_num,0) total_number_of_invitation_new_users,
        nvl(nd.new_user_complete_invitation_num,0) number_of_invitation_new_users
        
        from 
        order_data od 
        left join new_data nd on od.day = nd.day and od.shop_id = nd.shop_id
        left join new_data_merchant ndm on od.day = ndm.day and od.shop_id = ndm.shop_id
        ;
        
        ''',
    schema='ofood_bi',
    dag=dag)

create_crm_data = BashOperator(
    task_id='create_crm_data',
    bash_command="""
        dt="{{ ds }}"

        crm_sql="
            create temporary function isInArea as 'com.oride.udf.IsInArea' 
            USING JAR 'hdfs://node4.datalake.opay.com:8020/tmp/udf-1.0-SNAPSHOT-jar-with-dependencies.jar';
            
            insert overwrite table ofood_bi.ofood_area_shop_metrics_info partition(dt = '${dt}')
            select 
            d.bd_id,
            d.name,
            d.area_name,
            d.shop_id,
            d.title,
            d.total_number_of_merchants,
            d.total_number_of_new_merchants,
            d.trade_number_of_merchants,
            d.have_price_number_of_merchants,
            d.number_of_new_merchants,
            d.number_of_pay_orders,
            d.number_of_new_users,
            d.gmv,
            d.paid_in_amount,
            d.platform_subsidies,
            d.amount_and_first_price,
            d.net_turnover,
            d.total_number_of_order,
            d.number_of_valid_order,
            d.number_of_cancel_order_before_payment,
            d.number_of_cancel_order_after_payment,
            d.total_number_of_invitation_new_users,
            d.number_of_invitation_new_users
            
            from 
            (
                select 
                d.bd_id,
                d.name,
                d.area_id,
                d.area_name,
                d.shop_id,
                d.title,
                d.total_number_of_merchants,
                d.total_number_of_new_merchants,
                d.trade_number_of_merchants,
                d.have_price_number_of_merchants,
                d.number_of_new_merchants,
                d.number_of_pay_orders,
                d.number_of_new_users,
                d.gmv,
                d.paid_in_amount,
                d.platform_subsidies,
                d.amount_and_first_price,
                d.net_turnover,
                d.total_number_of_order,
                d.number_of_valid_order,
                d.number_of_cancel_order_before_payment,
                d.number_of_cancel_order_after_payment,
                d.total_number_of_invitation_new_users,
                d.number_of_invitation_new_users,
                row_number() over(partition by d.shop_id order by d.area_id) order_by
                from 
                (
                select 
                t.bd_id,
                t.name,
                t.area_id,
                t.area_name,
                s.shop_id,
                s.title,
                nvl(count(if(s.is_open = 1,s.shop_id,null)),0) total_number_of_merchants,
                nvl(count(if(s.is_first_placed_order = 1,s.shop_id,null)),0) total_number_of_new_merchants,
                nvl(count(if(s.number_of_valid_order > 0,s.shop_id,null)),0) trade_number_of_merchants,
                nvl(count(if(s.gmv > 0,s.shop_id,null)),0) have_price_number_of_merchants,
                nvl(count(if(s.is_first_completed_order = 1,s.shop_id,null)),0) number_of_new_merchants,
                nvl(sum(s.pay_order_num),0) number_of_pay_orders,
                nvl(sum(s.number_of_new_users),0) number_of_new_users,
                nvl(sum(s.gmv),0) gmv,
                nvl(sum(s.amount_price_sum),0) paid_in_amount,
                nvl(sum(s.cost_price_sum),0) platform_subsidies,
                nvl(sum(s.amount_and_first_price_sum),0) amount_and_first_price,
                nvl(sum(s.net_turnover),0) net_turnover,
                nvl(sum(s.total_number_of_order),0) total_number_of_order,
                nvl(sum(s.number_of_valid_order),0) number_of_valid_order,
                nvl(sum(s.before_pay_cancel_num),0) number_of_cancel_order_before_payment,
                nvl(sum(s.after_pay_cancel_num),0) number_of_cancel_order_after_payment,
                nvl(sum(s.total_number_of_invitation_new_users),0) total_number_of_invitation_new_users,
                nvl(sum(s.number_of_invitation_new_users),0) number_of_invitation_new_users
                
                
                from     
                (
                    select 
                    a.dt,
                    a.id bd_id,
                    a.name,
                    b.id area_id,
                    b.area_name,
                    b.points
                    from ofood_dw_ods.ods_sqoop_bd_bd_bd_fence_df b
                    join ofood_dw_ods.ods_sqoop_bd_bd_admin_users_df a on a.dt = '${dt}' and b.uid = a.id and job_id = 4
                    where b.dt = '${dt}'
                ) t 
                left join ofood_bi.ofood_order_shop_metrics_report s on s.dt = t.dt 
                where isInArea(t.points,s.lat/1000000,s.lng/1000000) = 1
                group by t.dt,t.bd_id,t.name,t.area_id,t.area_name,s.shop_id,s.title
                ) d 
            ) d
            where d.order_by = 1
            ;
"
        echo ${crm_sql}
        hive -e "${crm_sql}" 
    """,
    dag=dag,
)

insert_crm_metrics = HiveToMySqlTransfer(
    task_id='insert_crm_metrics',
    sql=""" 
        select 
        null,
        bd_id,
        dt,
        username,
        area_name,
        shop_id,
        title,
        total_number_of_merchants,
        total_number_of_new_merchants,
        trade_number_of_merchants,
        have_price_number_of_merchants,
        number_of_new_merchants,
        number_of_pay_orders,
        number_of_new_users,
        gmv,
        paid_in_amount,
        platform_subsidies,
        amount_and_first_price,
        net_turnover,
        total_number_of_order,
        number_of_valid_order,
        number_of_cancel_order_before_payment,
        number_of_cancel_order_after_payment,
        total_number_of_invitation_new_users,
        number_of_invitation_new_users
        
        from 
        ofood_bi.ofood_area_shop_metrics_info
        where dt = '{{ ds }}'
        

        """,
    mysql_conn_id='mysql_bi',
    mysql_table='ofood_area_shop_metrics_info',
    dag=dag)



def send_csv_file(ds, ds_nodash, **kwargs):
    cursor = get_hive_cursor()
    sql = """
        select  
        dt,
        shop_id,
        title,
        number_of_valid_order,
        gmv,
        net_turnover,
        total_number_of_new_users,
        number_of_new_users,
        punctual_arrival_number_of_order,
        negative_comment_number_of_order,
        total_number_of_order,
        before_pay_cancel_num,
        number_of_cancel_order_auto,
        number_of_cancel_order_user,
        number_of_cancel_order_shop
        from ofood_bi.ofood_order_shop_metrics_report 
        where dt = '{dt}' and total_number_of_order > 0
    
    """.format(dt=ds, ds=ds_nodash)

    headers = [
        'day',
        'shop_id',
        'title',
        'number_of_valid_order',
        'gmv',
        'net_turnover',
        'total_number_of_new_users',
        'number_of_new_users',
        'punctual_arrival_number_of_order',
        'negative_comment_number_of_order',
        'total_number_of_order',
        'number_of_cancel_order_before_payment',
        'number_of_cancel_order_auto',
        'number_of_cancel_order_user',
        'number_of_cancel_order_shop'
    ]

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    file_name = '/tmp/ofood_shop_metrics_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)

    # send mail
    email_to = Variable.get("ofood_bd_shop_detail_report_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    email_subject = 'ofood餐厅维度每日数据_{dt}'.format(dt=ds)
    email_body = 'ofood餐厅维度每日数据'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

validate_partition_data >> jh_waimai_validate_task >> insert_shop_metrics
validate_partition_data >> jh_waimai_order_validate_task >> insert_shop_metrics
validate_partition_data >> jh_shop_validate_task >> insert_shop_metrics
validate_partition_data >> jh_order_validate_task >> insert_shop_metrics
validate_partition_data >> jh_order_log_validate_task >> insert_shop_metrics
validate_partition_data >> jh_order_time_validate_task >> insert_shop_metrics
validate_partition_data >> jh_waimai_comment_validate_task >> insert_shop_metrics

validate_partition_data >> bd_admin_users_validate_task >> insert_shop_metrics
validate_partition_data >> bd_bd_fence_validate_task >> insert_shop_metrics

insert_shop_metrics >> create_crm_data >> insert_crm_metrics
