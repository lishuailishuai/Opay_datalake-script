# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
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
    schedule_interval="10 03 * * *",
    default_args=args)

cursor = get_hive_cursor()


def send_csv_file(ds, ds_nodash, **kwargs):
    sql = """
        with 
        order_data as (
            select
            o.day day ,
            s.title,
            o.shop_id,
            count(o.order_id) order_num,
            count(if(o.order_status = 8 and s.shop_id is not null,o.order_id,null)) success_order_num,
            sum(if(o.order_status = 8 and s.shop_id is not null,o.total_price,0)) trade_price_sum,
            sum(if(o.order_status = 8 and s.shop_id is not null,o.total_price - o.order_youhui - o.first_youhui,0)) actual_trade_price_sum,
            count(if(o.order_status = 8 and t.order_id is not null and  (t.order_compltet_time - o.pay_time)/60 <= 40,o.order_id,null)) in_time_order_num,
            count(if(o.order_status = 8 and t.order_id is not null and w.order_id is not null 
            and  (t.order_compltet_time - o.pay_time)/60 <= 40 and w.score_peisong <= 2,o.order_id,null)) in_time_negative_order_num,
            count(if(o.order_status = -1 and o.pay_status = 0,o.order_id,null)) cancel_num,
            count(if(o.order_status = -2 and lu.order_id is not null,o.order_id,null)) user_cancel_num,
            count(if(o.order_status = -2 and lm.order_id is not null,o.order_id,null)) merchant_cancel_num
            from 
            ofood_dw.ods_sqoop_base_jh_order_df o
            left join ofood_dw.ods_sqoop_base_jh_shop_df s on s.dt = '{dt}' and  o.shop_id = s.shop_id
            left join ofood_dw.ods_sqoop_base_jh_order_time_df t on t.dt = '{dt}' and  o.order_id = t.order_id
            left join ofood_dw.ods_sqoop_base_jh_waimai_comment_df w on w.dt = '{dt}' and  o.order_id = w.order_id
            left join (
                select 
                order_id
                from 
                ofood_dw.ods_sqoop_base_jh_order_log_df
                where dt = '{dt}' and status = -1 
                and (
                    log like '%User cancelling order%'
                    or log like '%用户取消订单%'
                )
            ) lu on o.order_id = lu.order_id
            left join (
                select 
                order_id
                from 
                ofood_dw.ods_sqoop_base_jh_order_log_df
                where dt = '{dt}' and status = -1 
                and log like '%Merchant cancelling order%'
            ) lm on o.order_id = lm.order_id
            
            where o.dt = '{dt}' and o.day = '{ds}'
            group by o.day,s.title,o.shop_id
        ),
        
        new_data as 
        (
            select 
            from_unixtime(unix_timestamp('{ds}', 'yyyyMMdd'),'yyyyMMdd') day,
            s.shop_id,
            nvl(count(distinct if(s.ft = '{ds}',s.dh,null)),0) new_user_place_num,
            nvl(count(distinct if(s.ft = '{ds}' and s.order_status = 8,s.dh,null)),0) new_user_complete_num
            from 
            (
            select 
            t.shop_id,
            t.dh,
            t.order_status,
            t.ft,
            row_number() over(partition by t.dh,t.order_status order by t.ft) order_id
            from 
            (
            select 
            o.shop_id,
            o.mobile dh,
            o.order_status,
            from_unixtime(min(dateline),'yyyyMMdd') ft
            from 
            ofood_dw.ods_sqoop_base_jh_order_df o
            where o.dt = '{dt}'
            group by o.shop_id,o.mobile,o.order_status
            ) t
            ) s
            where s.order_id = 1
            group by s.shop_id
        ) 
        
        
        select 
        od.day ,
        od.shop_id shop_id,
        od.title title,
        od.success_order_num number_of_valid_order,
        od.trade_price_sum gmv,
        od.actual_trade_price_sum net_turnover,
        nvl(nd.new_user_place_num,0) total_number_of_new_users,
        nvl(nd.new_user_complete_num,0) number_of_new_users,
        od.in_time_order_num punctual_arrival_number_of_order,
        od.in_time_negative_order_num negative_comment_number_of_order,
        od.order_num total_number_of_order,
        od.cancel_num number_of_cancel_order_before_payment,
        (od.order_num - od.success_order_num - od.cancel_num - od.user_cancel_num - od.merchant_cancel_num) number_of_cancel_order_auto,
        od.user_cancel_num number_of_cancel_order_user,
        od.merchant_cancel_num number_of_cancel_order_shop
        from 
        order_data od 
        left join new_data nd on od.day = nd.day and od.shop_id = nd.shop_id
        order by shop_id
    
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
    email_to = Variable.get("ofood_metrics_report_receivers").split()
    email_subject = 'ofood餐厅维度每日数据_{dt}'.format(dt=ds)
    email_body = 'ofood餐厅维度每日数据'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')



send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

send_file_email
