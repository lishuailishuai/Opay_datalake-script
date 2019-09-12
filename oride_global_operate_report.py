# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.email import send_email
import logging
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
from utils.validate_metrics_utils import *
from constant.metrics_constant import *
from airflow.sensors.hive_partition_sensor import HivePartitionSensor

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_global_operate_report',
    schedule_interval="00 09 * * *",
    default_args=args)

# 熔断阻塞流程,配置依赖
app_oride_global_operate_report_d_task = HivePartitionSensor(
    task_id="app_oride_global_operate_report_d_task",
    table="app_oride_global_operate_report_d",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

def get_all_data_row(ds):
    tr_fmt = '''
           <tr>{row}</tr>
        '''
    row_fmt = '''
                        <td>{}</td>
                        <!--关键指标-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--乘客指标-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--司机指标-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--财务-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--系统-->
                        <td>{}</td>
                '''
    sql = '''
            select dt,
            ride_order_cnt, --当日下单数
            '-' as order_cnt_lfw, --下单数（近四周同期均值）
            valid_ord_cnt,  --当日有效下单量
            finish_pay, --当日支付完单数
            finish_order_cnt, --当日完单量
            '-' as finish_order_cnt_lfw, --完单数（近四周同期均值）
            concat(cast(nvl(round(finish_order_cnt*100/ride_order_cnt,1),0) as string),'%') as finish_order_rate, --完单率
            '-' as finish_order_rate_lfw, --完单率（近四周）
            beckoning_num, --当日招手停完单数
            new_users, --当日注册乘客数
            act_users, --当日活跃乘客数
            ord_users, --当日下单乘客数
            first_finished_users,  --当日首次完单乘客数
            finished_users,  --当日完单老客数
            new_user_ord_cnt, --当日注册乘客下单量
            new_user_finished_cnt, --当日注册乘客完单量
            concat(cast(nvl(round(online_paid_users*100/paid_users,1),0) as string),'%') as online_paid_users_rate,  --当日线上支付乘客占比
            td_audit_finish_driver_num, --当日审核通过司机数
            online_driver_num, --当日在线司机数
            request_driver_num, --当日接单司机数
            finish_order_driver_num, --当日完单司机数
            price,  --订单应付总额,状态4，5
            new_user_gmv, -- 当日新注册乘客完单gmv，状态4，5
            concat(cast(nvl(round((recharge_amount+reward_amount)*100/price,1),0) as string),'%') as b_subsidy_rate,  --b端补贴率
            concat(cast(nvl(round((price-pay_amount)*100/price,1),0) as string),'%') as c_subsidy_rate, --c端补贴率【gmv状态4，5；实付金额状态5】
            user_recharge_succ_balance, --每日用户充值真实金额
            recharge_users, --每日充值客户数
            map_request_num,  --地图调用次数
            concat(cast(nvl(round(opay_pay_failed_cnt*100/opay_pay_cnt,1),0) as string),'%') as opay_pay_failed_rate --opay支付失败占比
            FROM oride_dw.app_oride_global_operate_report_d
            WHERE dt between '{start_date}' and '{dt}'
            AND country_code='nal'
            AND city_id=-10000
            AND product_id=-10000
            order by dt desc 
        '''.format(dt=ds,
                   start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
    return row_html

def send_report_email(ds, **kwargs):
    #logging.info("receivers:%s" % Variable.get("oride_global_operate_report_receivers"))
    html_fmt = '''
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
                    background-color: #f9cb9c;
                    //color: white;
                    width: 100px;
                    color: #000000;
                }}
            </style>
            </head>
            <body>
                <table width="100%" class="table">
                    <caption>
                        <h3>全部城市</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="9" style="text-align: center;">关键指标</th>
                            <th colspan="8" style="text-align: center;">乘客指标</th>
                            <th colspan="4" style="text-align: center;">司机指标</th>
                            <th colspan="7" style="text-align: center;">财务</th>
                            <th colspan="1" style="text-align: center;">系统</th>

                        </tr>
                        <tr>
                            <th>日期</th>
                            <!--关键指标-->
                            <th>下单数</th>
                            <th>下单数（近四周均值）</th>
                            <th>有效下单数</th>
                            <th>支付完单数</th>
                            <th>完单数</th>
                            <th>完单数（近四周均值）</th>
                            <th>完单率</th>
                            <th>完单率（近四周均值）</th>
                            <th>招手停完单数</th>
                            <!--乘客指标-->
                            <th>注册乘客数</th>
                            <th>活跃乘客数</th>
                            <th>下单乘客数</th>
                            <th>首次完单乘客数</th>
                            <th>完单老客数</th>
                            <th>当日注册乘客下单数</th>
                            <th>当日注册乘客完单数</th>
                            <th>线上支付乘客占比</th>
                            <!--司机指标-->
                            <th>审核通过司机数</th>
                            <th>在线司机数</th>
                            <th>接单司机数</th>
                            <th>完单司机数</th>
                            <!--财务指标-->
                            <th>GMV</th>
                            <th>当日注册乘客完单gmv</th>
                            <th>B端补贴率</th>
                            <th>C端补贴率</th>
                            <th>每日充值真实金额</th>
                            <th>每日充值人数</th>
                            <th>地图调用次数</th>
                            <!--系统-->
                            <th>opay支付失败订单占比</th>
                        </tr>
                    </thead>
                    <tbody>
                    {rows}
                    </tbody>
                </table>
        </body>
        </html>
        '''
    html = html_fmt.format(rows=get_all_data_row(ds))

    # send mail
    # email_to = Variable.get("oride_global_operate_report_receivers").split()
    email_to = ['lili.chen@opay-inc.com','min.yuan@opay-inc.com']

    # send mail
    email_subject = 'oride全局运营日报_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

app_oride_global_operate_report_d_task >> send_report
