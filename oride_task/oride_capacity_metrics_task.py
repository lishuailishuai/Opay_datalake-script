# -*- coding: utf-8 -*-
"""
调度算法效果监控指标新版-DW模型 2019-09-03
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
from datetime import datetime, timedelta
import re
import logging
from utils.validate_metrics_utils import *
import time
from airflow.models import Variable
from airflow.utils.email import send_email

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 9, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_capacity_metrics_task',
    schedule_interval="30 04 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)

"""
##----依赖数据源---##
"""
dependence_app_oride_capacity_base_d = UFileSensor(
    task_id='dependence_app_oride_capacity_base_d',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/app_oride_capacity_base_d",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


def send_capacity_report(ds, **kargs):
    mail_html = '''
    <html>
    <head>
    <title></title>
    <style type="text/css">
        table
        {{
            font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
            border-collapse: collapse;
            margin: 0 auto;
            text-align: left;
            align:left;
            font-size: 13px;
        }}
        table td, table th
        {{
            border: 1px solid #000000;
            color: #000000;
            height: 30px;
            padding: 5px 10px 5px 5px;
        }}
    </style>
    </head>
    <body>
        <!-- 全国全业务 -->
        <table width="100%" class="table">
            <thead>
                {header_all}
                {table_title}
            </thead>
            <tbody>
            {table_rows_all}
            </tbody>
        </table>
        <hr>
        <!-- 城市全业务 -->
        <table width="100%" class="table">
            <thead>
                {header_city}
                {table_title}
            </thead>
            <tbody>
            {table_rows_city}
            </tbody>
        </table>
        <hr>
        <!-- Green业务 -->
        <table width="100%" class="table">
            <thead>
                {header_green}
                {table_title}
            </thead>
            <tbody>
            {table_rows_green}
            </tbody>
        </table>
        <hr>
        <!-- Street业务 -->
        <table width="100%" class="table">
            <thead>
                {header_street}
                {table_title}
            </thead>
            <tbody>
            {table_rows_street}
            </tbody>
        </table>
        <hr>
        <!-- Otrike业务 -->
        <table width="100%" class="table">
            <thead>
                {header_otrike}
                {table_title}
            </thead>
            <tbody>
            {table_rows_otrike}
            </tbody>
        </table>
    </body>
    </html>
    '''

    table_title = '''
    <tr>
        <th style="text-align: center;">日期</th>
        <th style="text-align: center;">城市</th>
        <th style="text-align: center;">业务线</th>
        <th style="text-align: center;">平均播单距离</th>
        <th style="text-align: center;">平均接驾距离(完单)</th>
        <th style="text-align: center;">平均接驾距离(应答)</th>
        <th style="text-align: center;">调度服从率</th>
        <th style="text-align: center;">应答率</th>
        <th style="text-align: center;">平均应答时长(完单)</th>
        <th style="text-align: center;">平均应答时长(应答)</th>
        <th style="text-align: center;">乘客应答后取消率</th>
        <th style="text-align: center;">司机应答后取消率</th>
        <th style="text-align: center;">TPH</th>
        <th style="text-align: center;">人均在线时长</th>
        <th style="text-align: center;">计费时长占比</th>
    </tr>
    '''

    table_rows = '''
    <tr>
        <td>{dt}</td>
        <td>{city}</td>
        <td>{product}</td>
        <td>{broadcast_distance}</td>
        <td>{pickup_distance_done}</td>
        <td>{pickup_distance_take}</td>
        <td>{dispatch_obey_rate}%</td>
        <td>{take_rate}%</td>
        <td>{take_order_dur_done}</td>
        <td>{take_order_dur_take}</td>
        <td>{passanger_after_cancel_rate}%</td>
        <td>{driver_after_cancel_rate}%</td>
        <td>{tph_macro}</td>
        <td>{online_range}</td>
        <td>{billing_order_dur_rate}%</td>
    </tr>
    '''

    header_all = '''
            <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 全国</th></tr>
        '''

    header_city = '''
                <tr style="background-color:#bf9001"><th colspan="15">分城市宏观指标 - 全部业务线</th></tr>
            '''

    header_green = '''
                <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Green</th></tr>
            '''

    header_street = '''
                <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Street</th></tr>
            '''

    header_otrike = '''
    		<tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Otrike</th></tr>
    	'''

    cursor = get_hive_cursor()

    """
        全部城市合并，不区分业务线
    
    """
    all_country_sql = """
        SELECT city_id,
               'All' as city,
               product_id,
               broadcast_dis_avg,
               --平均播单距离
        
               finish_order_pick_up_dis_avg,
               --平均接驾距离(完单)
        
               request_order_pick_up_dis_avg,
               --平均接驾距离(应答)
        
               obey_rate,
               --调度服从率
        
               request_order_rate,
               --应答率
        
               finish_order_request_time_avg,
               --平均应答时长(完单)
        
               request_order_request_time_avg,
               --平均应答时长(应答)
        
               passanger_after_cancel_order_rate,
               --乘客应答后取消率
        
               driver_after_cancel_order_rate,
               --司机应答后取消率
        
               TPH,
               --TPH
        
               online_time_avg,
               --人均在线时长
        
               billing_time_rate --计费时长占比
        FROM oride_dw.app_oride_capacity_base_d
        WHERE dt='{pt}'
          AND city_id=-10000
          AND product_id=-10000
          and country_code='nal'
        GROUP BY city_id,
                 product_id,
                 broadcast_dis_avg,
                 finish_order_pick_up_dis_avg,
                 request_order_pick_up_dis_avg,
                 obey_rate,
                 request_order_rate,
                 finish_order_request_time_avg,
                 request_order_request_time_avg,
                 passanger_after_cancel_order_rate,
                 driver_after_cancel_order_rate,
                 TPH,
                 online_time_avg,
                 billing_time_rate
    
    """.format(pt=ds)

    cursor.execute(all_country_sql)
    all_country_data = cursor.fetchall()

    table_rows_all = ''
    for (city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in all_country_data:
        table_rows_all += table_rows.format(
            dt=ds,
            city=city,
            product=product,
            broadcast_distance=broadcast_distance,
            pickup_distance_done=pickup_distance_done,
            pickup_distance_take=pickup_distance_take,
            dispatch_obey_rate=dispatch_obey_rate,
            take_rate=take_rate,
            take_order_dur_done=take_order_dur_done,
            take_order_dur_take=take_order_dur_take,
            passanger_after_cancel_rate=passanger_after_cancel_rate,
            driver_after_cancel_rate=driver_after_cancel_rate,
            tph_macro=tph_macro,
            online_range=online_range,
            billing_order_dur_rate=billing_order_dur_rate
        )

    """
        全部城市，不区分业务线

    """
    city_all_product_sql = """
        SELECT d.city_id,
               c.city_name,
               d.product_id,
               d.broadcast_dis_avg,
               --平均播单距离

               d.finish_order_pick_up_dis_avg,
               --平均接驾距离(完单)

               d.request_order_pick_up_dis_avg,
               --平均接驾距离(应答)

               d.obey_rate,
               --调度服从率

               d.request_order_rate,
               --应答率

               d.finish_order_request_time_avg,
               --平均应答时长(完单)

               d.request_order_request_time_avg,
               --平均应答时长(应答)

               d.passanger_after_cancel_order_rate,
               --乘客应答后取消率

               d.driver_after_cancel_order_rate,
               --司机应答后取消率

               d.TPH,
               --TPH

               d.online_time_avg,
               --人均在线时长

               billing_time_rate --计费时长占比
        FROM oride_dw.app_oride_capacity_base_d as d 
        join(
            select 
            city_id,
            city_name
            from oride_dw.dim_oride_city
            where dt = '{pt}'
            and city_id <> 999001
        ) as c on d.city_id = c.city_id
        WHERE d.dt='{pt}'
          AND d.product_id=-10000
          and d.country_code='nal'
        GROUP BY d.city_id,
                 c.city_name,
                 d.product_id,
                 d.broadcast_dis_avg,
                 d.finish_order_pick_up_dis_avg,
                 d.request_order_pick_up_dis_avg,
                 d.obey_rate,
                 d.request_order_rate,
                 d.finish_order_request_time_avg,
                 d.request_order_request_time_avg,
                 d.passanger_after_cancel_order_rate,
                 d.driver_after_cancel_order_rate,
                 d.TPH,
                 d.online_time_avg,
                 d.billing_time_rate

        """.format(pt=ds)

    cursor.execute(city_all_product_sql)
    city_all_product_data = cursor.fetchall()

    table_rows_city = ''
    for (city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in city_all_product_data:
        table_rows_city += table_rows.format(
            dt=ds,
            city='All',
            product=product,
            broadcast_distance=broadcast_distance,
            pickup_distance_done=pickup_distance_done,
            pickup_distance_take=pickup_distance_take,
            dispatch_obey_rate=dispatch_obey_rate,
            take_rate=take_rate,
            take_order_dur_done=take_order_dur_done,
            take_order_dur_take=take_order_dur_take,
            passanger_after_cancel_rate=passanger_after_cancel_rate,
            driver_after_cancel_rate=driver_after_cancel_rate,
            tph_macro=tph_macro,
            online_range=online_range,
            billing_order_dur_rate=billing_order_dur_rate
        )

    """
        专车业务，全部城市

    """
    city_green_sql = """
        SELECT d.city_id,
               c.city_name,
               d.product_id,
               d.broadcast_dis_avg,
               --平均播单距离

               d.finish_order_pick_up_dis_avg,
               --平均接驾距离(完单)

               d.request_order_pick_up_dis_avg,
               --平均接驾距离(应答)

               d.obey_rate,
               --调度服从率

               d.request_order_rate,
               --应答率

               d.finish_order_request_time_avg,
               --平均应答时长(完单)

               d.request_order_request_time_avg,
               --平均应答时长(应答)

               d.passanger_after_cancel_order_rate,
               --乘客应答后取消率

               d.driver_after_cancel_order_rate,
               --司机应答后取消率

               d.TPH,
               --TPH

               d.online_time_avg,
               --人均在线时长

               d.billing_time_rate --计费时长占比
        FROM oride_dw.app_oride_capacity_base_d as d
        join(
            select 
            city_id,
            city_name
            from oride_dw.dim_oride_city
            where dt = '{pt}'
            and city_id <> 999001
        ) as c on d.city_id = c.city_id
        WHERE d.dt='{pt}'
          AND d.product_id=1
          and d.country_code='nal'
        GROUP BY d.city_id,
                 c.city_name,
                 d.product_id,
                 d.broadcast_dis_avg,
                 d.finish_order_pick_up_dis_avg,
                 d.request_order_pick_up_dis_avg,
                 d.obey_rate,
                 d.request_order_rate,
                 d.finish_order_request_time_avg,
                 d.request_order_request_time_avg,
                 d.passanger_after_cancel_order_rate,
                 d.driver_after_cancel_order_rate,
                 d.TPH,
                 d.online_time_avg,
                 d.billing_time_rate

            """.format(pt=ds)

    cursor.execute(city_green_sql)
    city_green_product_data = cursor.fetchall()

    table_rows_green = ''
    for (city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in city_green_product_data:
        table_rows_green += table_rows.format(
            dt=ds,
            city=city,
            product=product,
            broadcast_distance=broadcast_distance,
            pickup_distance_done=pickup_distance_done,
            pickup_distance_take=pickup_distance_take,
            dispatch_obey_rate=dispatch_obey_rate,
            take_rate=take_rate,
            take_order_dur_done=take_order_dur_done,
            take_order_dur_take=take_order_dur_take,
            passanger_after_cancel_rate=passanger_after_cancel_rate,
            driver_after_cancel_rate=driver_after_cancel_rate,
            tph_macro=tph_macro,
            online_range=online_range,
            billing_order_dur_rate=billing_order_dur_rate
        )

    """
        快车业务，全部城市

    """

    city_street_sql = """
        SELECT d.city_id,
               c.city_name,
               d.product_id,
               d.broadcast_dis_avg,
               --平均播单距离

               d.finish_order_pick_up_dis_avg,
               --平均接驾距离(完单)

               d.request_order_pick_up_dis_avg,
               --平均接驾距离(应答)

               d.obey_rate,
               --调度服从率

               d.request_order_rate,
               --应答率

               d.finish_order_request_time_avg,
               --平均应答时长(完单)

               d.request_order_request_time_avg,
               --平均应答时长(应答)

               d.passanger_after_cancel_order_rate,
               --乘客应答后取消率

               d.driver_after_cancel_order_rate,
               --司机应答后取消率

               d.TPH,
               --TPH

               d.online_time_avg,
               --人均在线时长

               billing_time_rate --计费时长占比
        FROM oride_dw.app_oride_capacity_base_d as d
        join(
            select 
            city_id,
            city_name
            from oride_dw.dim_oride_city
            where dt = '{pt}'
            and city_id <> 999001
        ) as c on d.city_id = c.city_id
        WHERE d.dt='{pt}'
          AND d.product_id=2
          and d.country_code='nal'
        GROUP BY d.city_id,
                 c.city_name,
                 d.product_id,
                 d.broadcast_dis_avg,
                 d.finish_order_pick_up_dis_avg,
                 d.request_order_pick_up_dis_avg,
                 d.obey_rate,
                 d.request_order_rate,
                 d.finish_order_request_time_avg,
                 d.request_order_request_time_avg,
                 d.passanger_after_cancel_order_rate,
                 d.driver_after_cancel_order_rate,
                 d.TPH,
                 d.online_time_avg,
                 d.billing_time_rate

                """.format(pt=ds)

    cursor.execute(city_street_sql)
    city_street_product_data = cursor.fetchall()

    table_rows_street = ''
    for (city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in city_street_product_data:
        table_rows_street += table_rows.format(
            dt=ds,
            city=city,
            product=product,
            broadcast_distance=broadcast_distance,
            pickup_distance_done=pickup_distance_done,
            pickup_distance_take=pickup_distance_take,
            dispatch_obey_rate=dispatch_obey_rate,
            take_rate=take_rate,
            take_order_dur_done=take_order_dur_done,
            take_order_dur_take=take_order_dur_take,
            passanger_after_cancel_rate=passanger_after_cancel_rate,
            driver_after_cancel_rate=driver_after_cancel_rate,
            tph_macro=tph_macro,
            online_range=online_range,
            billing_order_dur_rate=billing_order_dur_rate
        )

    """
        otrike业务，全部城市

    """

    city_otrike_sql = """
        SELECT d.city_id,
               c.city_name,
               d.product_id,
               d.broadcast_dis_avg,
               --平均播单距离

               d.finish_order_pick_up_dis_avg,
               --平均接驾距离(完单)

               d.request_order_pick_up_dis_avg,
               --平均接驾距离(应答)

               d.obey_rate,
               --调度服从率

               d.request_order_rate,
               --应答率

               d.finish_order_request_time_avg,
               --平均应答时长(完单)

               d.request_order_request_time_avg,
               --平均应答时长(应答)

               d.passanger_after_cancel_order_rate,
               --乘客应答后取消率

               d.driver_after_cancel_order_rate,
               --司机应答后取消率

               d.TPH,
               --TPH

               d.online_time_avg,
               --人均在线时长

               d.billing_time_rate --计费时长占比
        FROM oride_dw.app_oride_capacity_base_d as d
        join(
            select 
            city_id,
            city_name
            from oride_dw.dim_oride_city
            where dt = '{pt}'
            and city_id <> 999001
        ) as c on d.city_id = c.city_id
        WHERE d.dt='{pt}'
          AND d.product_id=3
          and d.country_code='nal'
        GROUP BY d.city_id,
                 c.city_name,
                 d.product_id,
                 d.broadcast_dis_avg,
                 d.finish_order_pick_up_dis_avg,
                 d.request_order_pick_up_dis_avg,
                 d.obey_rate,
                 d.request_order_rate,
                 d.finish_order_request_time_avg,
                 d.request_order_request_time_avg,
                 d.passanger_after_cancel_order_rate,
                 d.driver_after_cancel_order_rate,
                 d.TPH,
                 d.online_time_avg,
                 d.billing_time_rate

                    """.format(pt=ds)

    cursor.execute(city_otrike_sql)
    city_otrike_product_data = cursor.fetchall()

    table_rows_otrike = ''
    for (city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in city_otrike_product_data:
        table_rows_otrike += table_rows.format(
            dt=ds,
            city=city,
            product=product,
            broadcast_distance=broadcast_distance,
            pickup_distance_done=pickup_distance_done,
            pickup_distance_take=pickup_distance_take,
            dispatch_obey_rate=dispatch_obey_rate,
            take_rate=take_rate,
            take_order_dur_done=take_order_dur_done,
            take_order_dur_take=take_order_dur_take,
            passanger_after_cancel_rate=passanger_after_cancel_rate,
            driver_after_cancel_rate=driver_after_cancel_rate,
            tph_macro=tph_macro,
            online_range=online_range,
            billing_order_dur_rate=billing_order_dur_rate
        )

    mail_html_content = mail_html.format(
        header_all=header_all,
        header_city=header_city,
        header_green=header_green,
        header_street=header_street,
        header_otrike=header_otrike,
        table_title=table_title,
        table_rows_all=table_rows_all,
        table_rows_city=table_rows_city,
        table_rows_green=table_rows_green,
        table_rows_street=table_rows_street,
        table_rows_otrike=table_rows_otrike
    )

    # send mail
    # email_to = ['bigdata@opay-inc.com']
    email_to = ['nan.li@opay-inc.com']
    email_subject = 'oride调度算法指标-DW数仓模型构建_{}'.format(ds)
    send_email(
        email_to,
        email_subject,
        mail_html_content,
        mime_charset='utf-8'
    )


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_capacity_report,
    provide_context=True,
    dag=dag
)

dependence_app_oride_capacity_base_d >> sleep_time >> send_report
