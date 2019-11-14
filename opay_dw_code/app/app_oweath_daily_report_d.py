# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_hive_cursor, get_db_conn, get_pika_connection, get_redis_connection
from airflow.hooks.hive_hooks import HiveCliHook
from airflow import macros
import logging
from airflow.models import Variable
import pandas as pd
import io
import requests
import os
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

args = {
    'owner': 'shuzhen.liu',
    'start_date': datetime(2019, 7, 16),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_owealth_daily_report_d',
    schedule_interval="30 01 * * *",
    default_args=args)




def send_funnel_report_email(ds, **kwargs):
    sql = '''
        SELECT
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
            nvl(oride_cllick_request_event_counter,0),
            nvl(estimated_price_cllick_request_event_counter,0),
            concat(nvl(round(estimated_price_cllick_request_event_counter/oride_cllick_request_event_counter*100, 2),0),'%') as ep_vs_oc,
            concat(nvl(round(estimated_price_cllick_request_event_counter_lfw/oride_cllick_request_event_counter_lfw*100, 2),0),'%')as ep_vs_oc_lfw,
            concat(nvl(round(request_a_ride_click_c_new/estimated_price_cllick_request_event_counter*100, 2),0),'%') as rq_vs_ep,
            concat(nvl(round(request_a_ride_click_lfw_c_new/estimated_price_cllick_request_event_counter_lfw*100, 2),0),'%') as rq_vs_ep_lfw,
            nvl(request_num,0),
            nvl(request_num_lfw,0),
            nvl(round((request_num-push_num)/request_num*100, 2),0) as no_push_rate,
            nvl(round(before_take_cancel_num/request_num*100, 2),0) as before_take_cancel_rate,
            nvl(round(before_take_cancel_num_lfw/request_num_lfw*100, 2),0) as before_take_cancel_lfw_rate,
            nvl(round(take_num/request_num*100, 2),0) as take_rate,
            nvl(round(take_num_lfw/request_num_lfw*100, 2),0) as take_lfw_rate,
            nvl(round(after_take_cancel_num/request_num*100, 2),0) as after_take_cancel_rate,
            nvl(round(after_take_cancel_num_lfw/request_num_lfw*100, 2),0) as after_take_cancel_lfw_rate,
            nvl(round(driver_cancel_num/request_num*100, 2),0) as driver_cancel_rate,
            nvl(round(driver_cancel_num_lfw/request_num_lfw*100, 2),0) as driver_cancel_lfw_rate,
            nvl(completed_num,0),
            nvl(completed_num_lfw,0),
            nvl(round(completed_num/request_num*100, 2),0) as completed_rate,
            nvl(round(completed_num_lfw/request_num_lfw*100, 2),0) as completed_lfw_rate,
            nvl(pay_num,0),
            if (dt >= '2019-06-26',  nvl(round(pay_price_total/pay_num, 2),0), ''),
            if (dt >= '2019-06-26',  nvl(round(pay_amount_total/pay_num, 2),0), ''),
            nvl(request_usernum, '-')
        FROM
           oride_bi.oride_global_daily_report
        WHERE
            dt <= '{dt}'
        ORDER BY dt DESC
        LIMIT 14
    '''.format(dt=ds)


    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()


    html_fmt = '''
            <html>
            <head>
            <title></title>
            <style type="text/css">
                table
                {{
                    font-family:'黑体';
                    border-collapse: collapse;
                    margin: 0 auto;
                    text-align: left;
                    font-size:12px;
                    color:#29303A;
                }}
                table h2
                {{
                    font-size:20px;
                    color:#000000;
                }}
                .th_title
                {{
                    font-size:16px;
                    color:#000000;
                    text-align: center;
                }}
                table td, table th
                {{
                    border: 1px solid #000000;
                    color: #000000;
                    height: 30px;
                    padding: 5px 10px 5px 5px;
                }}
                table thead th
                {{
                    background-color: #DD7907;
                    //color: white;
                    width: 100px;
                }}
            </style>
            </head>
            <body>
                <table width="95%" class="table">
                    <caption>
                        <h2>owealth日报</h2>
                    </caption>
                </table>


                <table width="95%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="3" class="th_title">汇总数据</th>
                            <th colspan="3" class="th_title">agent数据</th>
                            <th colspan="3" class="th_title">customer数据</th>
                        </tr>
                        <tr>
                            <th>日期</th>
                            <!--汇总数据-->
                            
                            <th>累计总额</th>
                            <th>手动申购金额</th>
                            <th>赎回金额</th>
                            
                            <!--agent数据-->
                            <th>累计总额</th>
                            <th>手动申购金额</th>
                            <th>赎回金额</th>
                            
                            <!--customer数据-->
                            <th>累计总额</th>
                            <th>手动申购金额</th>
                            <th>赎回金额</th>
                            
                            
                        </tr>
                    </thead>
                    {rows}
                </table>

            </body>
            </html>
            '''

    row_html = ''

    if len(data_list) > 0:

        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#FFD8BF">{row}</tr>
        '''
        row_fmt = '''
                 <td>{0}</td>
                <!--呼叫前-->
                <td>{2}</td>
                <td>{3}</td>
                <td>{4}</td>
               
                <!--呼叫-应答-->
                <td>{26}</td>
                <td>{8}</td>
                <td>{9}</td>
                
                <!--完单-支付-->
                <td>{19}</td>
                <td>{20}</td>
                <td>{21}</td>
               
        '''

        for data in data_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)


    html = html_fmt.format(rows=row_html)
    # send mail

    email_to = ['shuzhen.liu@opay-inc.com']

    email_subject = 'oride订单漏斗模型_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return

send_funnel_report = PythonOperator(
    task_id='send_funnel_report',
    python_callable=send_funnel_report_email,
    provide_context=True,
    dag=dag
)



