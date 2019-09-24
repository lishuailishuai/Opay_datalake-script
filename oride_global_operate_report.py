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
    schedule_interval="30 02 * * *",
    default_args=args)

global_table_names = [
    'oride_dw.app_oride_global_operate_report_d',
    'oride_dw_ods.ods_sqoop_base_data_city_conf_df',
]

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
    weekend_tr_fmt = '''
                <tr style="background:#fff2cc">{row}</tr>
            '''
    weekday = {
        "1": "周一",
        "2": "周二",
        "3": "周三",
        "4": "周四",
        "5": "周五",
        "6": "周六",
        "7": "周日",
    }
    row_fmt = '''
                            <td>{}</td>
                            <!--{}-->
                            <!--关键指标-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td style="background:#d9d9d9">{}</td>
                            <td>{}</td>
                            <td style="background:#d9d9d9">{}</td>
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
                            <td style="background:#d9d9d9">{}</td>
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
                from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
                ride_order_cnt, --当日下单数
                if(dt>'2019-10-10',order_cnt_lfw,'-') as order_cnt_lfw, --下单数（近四周同期均值）
                valid_ord_cnt,  --当日有效下单量
                finish_pay, --当日支付完单数
                finish_order_cnt, --当日完单量
                if(dt>'2019-10-10',finish_order_cnt_lfw,'-') as finish_order_cnt_lfw, --完单数（近四周同期均值）
                concat(cast(nvl(round(finish_order_cnt*100/ride_order_cnt,1),0) as string),'%') as finish_order_rate, --完单率
                if(dt>'2019-10-10',concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) as string),'%'),'-') as finish_order_rate_lfw, --完单率（近四周）
                beckoning_num, --当日招手停完单数
                new_users, --当日注册乘客数
                act_users, --当日活跃乘客数
                ord_users, --当日下单乘客数
                first_finished_users,  --当日首次完单乘客数
                old_finished_users,  --当日完单老客数
                new_user_ord_cnt, --当日注册乘客下单量
                new_user_finished_cnt, --当日注册乘客完单量
                concat(cast(nvl(round(online_paid_users*100/paid_users,1),0) as string),'%') as online_paid_users_rate,  --当日线上支付乘客占比
                td_audit_finish_driver_num, --当日审核通过司机数
                td_online_driver_num, --当日在线司机数
                td_request_driver_num, --当日接单司机数
                td_finish_order_driver_num, --当日完单司机数
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
            week=data[1]
            print (week)
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)
            all_completed_num = data_list[0][6]-data_list[0][10]

    return row_html,all_completed_num


def get_product_rows(ds, all_completed_num,product_id):
    tr_fmt = '''
           <tr>{row}</tr>
        '''
    row_fmt1 = '''
                        <td>{}</td>
                        <!--{}-->
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
                        <td>{}</td>
                        <!--乘客指标-->
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
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--体验指标-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <!--财务指标-->
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>        
                '''
    row_fmt2 = '''
                            <td>{}</td>
                            <!--{}-->
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
                            <td>{}</td>
                            <!--乘客指标-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--{}-->
                            <td>{}</td>
                            <!--司机指标-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--体验指标-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--财务指标-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>        
                    '''

    if product_id == 3:
        row_fmt= row_fmt1
        product_id_1_limit=''
    elif product_id == 1:
        row_fmt = row_fmt2
        product_id_1_limit = 't1.city_id not in(1002,1005) and'
    else:
        row_fmt = row_fmt2
        product_id_1_limit = ''

    sql = '''
            SELECT t1.dt,
                 t1.city_id,
                 if(t1.city_id=-10000,'All',t2.name) AS city_name,
                 if(t1.city_id=1001,'-',nvl(t1.ride_order_cnt,0)) as ride_order_cnt, --当日下单数
                 if(dt>'2019-10-10',order_cnt_lfw,'-') AS order_cnt_lfw, --近四周同期下单数据
                 if(t1.city_id=1001,'-',nvl(t1.valid_ord_cnt,0)) as valid_ord_cnt, --有效下单量
                 nvl(t1.finish_pay,0) as finish_pay, --当日支付完单数
                 nvl(t1.finish_order_cnt,0) as finish_order_cnt, --当日完单量
                 if(dt>'2019-10-10',finish_order_cnt_lfw,'-') AS finish_order_cnt_lfw, --完单数（近四周同期均值）
                 concat(cast(nvl(round(t1.finish_order_cnt*100/t1.ride_order_cnt,1),0) AS string),'%') AS finish_order_rate, --完单率
                 if(dt>'2019-10-10',concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) as string),'%'),'-') AS finish_order_rate_lfw, --完单率（近四周）
                 concat(cast(nvl(round(if(t1.city_id=-10000,t1.finish_order_cnt*100/{all_completed_num},t1.finish_order_cnt*100/t1.city_total),1),0) AS string),'%') AS product_finish_order_rate,--业务完单占比
                 concat(cast(nvl(round(if(t1.city_id=-10000,t1.finish_order_cnt*100/t1.finish_order_cnt,t1.finish_order_cnt*2*100/sum(t1.finish_order_cnt) over(partition BY t1.product_id)),1),0) AS string),'%') AS city_finish_order_rate, --城市完单
                 nvl(t1.finished_users,0) as finished_users,--当日完单乘客数
                 nvl(if(product_id=3,t1.new_user_ord_cnt,t1.first_finished_users),0) as passenger_indictor_2, --1,2当日首次完单乘客数;3当日注册乘客下单数
                 nvl(t1.new_user_finished_cnt,0) as new_user_finished_cnt, --当日新注册乘客完单数
                 if(product_id=3,nvl(round(t1.pax_num/t1.finish_order_cnt,0),0),0) as passenger_indictor_4,
                 concat(cast(nvl(round(t1.online_paid_users*100/t1.paid_users,1),0) AS string),'%') AS online_paid_user_rate, --当日线上支付乘客占比
                 nvl(t1.td_audit_finish_driver_num,0) as td_audit_finish_driver_num, --当日审核通过司机数
                 nvl(t1.td_online_driver_num,0) as td_online_driver_num, --当日在线司机数
                 nvl(t1.td_request_driver_num,0) as td_request_driver_num, --当日接单司机数
                 nvl(t1.td_finish_order_driver_num,0) as td_finish_order_driver_num, --当日完单司机数
                 nvl(round(t1.finish_order_cnt/t1.td_finish_order_driver_num,0),0) AS avg_finish_order_cnt, --人均完单数
                 nvl(round(t1.finish_driver_online_dur/t1.td_finish_order_driver_num/3600,1),0) AS avg_driver_online_dur, --人均在线时长
                 concat(cast(nvl(round(t1.driver_billing_dur*100/t1.finish_driver_online_dur,1),0) AS string),'%') AS billing_dur_rate, --计费时长占比
                 nvl(round(t1.driver_pushed_order_cnt/t1.td_push_accpet_show_driver_num,0),0) AS avg_pushed_order_cnt, --人均推送订单数
                 nvl(round(t1.finish_order_cnt/t1.finish_driver_online_dur*3600,1),0) AS TPH,
                 nvl(round((t1.amount_pay_online+t1.amount_pay_offline+t1.recharge_amount+t1.reward_amount)/t1.finish_driver_online_dur,1),0) AS IPH,
                 nvl(round(t1.finish_take_order_dur/t1.finish_order_cnt,0),0) AS avg_take_order_dur,--平均应答时长
                 nvl(round(t1.finish_pick_up_dur/t1.finish_order_cnt,0),0) AS avg_pick_up_dur, --平均接驾时长
                 nvl(round(t1.billing_order_dur/t1.finish_order_cnt,0),0) AS avg_billing_order_dur,--平均计费时长
                 nvl(round(t1.finish_order_onride_dis/t1.finish_order_cnt,0),0) AS avg_order_onride_dis,--平均送驾距离
                 nvl(t1.price,0.0) AS gmv,
                 nvl(t1.new_user_gmv,0.0) as new_user_gmv, --当日注册乘客完单gmv
                concat(cast(nvl(round((t1.recharge_amount+t1.reward_amount)*100/t1.price,1),0) AS string),'%') AS b_subsidy_rate, --b端补贴率
                concat(cast(nvl(round((t1.price-t1.pay_amount)*100/t1.price,1),0) AS string),'%') AS c_subsidy_rate, --c端补贴率【gmv状态4，5；实付金额状态5】
                nvl(round(t1.pay_price/t1.finish_pay,1),0) AS avg_pay_price, --单均应付
                nvl(round(t1.pay_amount/t1.finish_pay,1),0) AS avg_pay_amount --单均实付
                from (SELECT sum(if(product_id not in(99,-10000),finish_order_cnt,0)) over(partition BY city_id) AS city_total,*
                      FROM oride_dw.app_oride_global_operate_report_d WHERE dt='{dt}') t1
                   LEFT JOIN
                  (SELECT id,
                          name
                   FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df
                   WHERE dt='{dt}') t2 ON t1.city_id=t2.id
                   where {product_id_1_limit}
                     t1.country_code='nal' --某个业务线汇总及城市明细
                     AND t1.product_id={product_id}
                ORDER BY t1.city_id ASC
        '''.format(dt=ds,
                   all_completed_num=all_completed_num,
                   product_id_1_limit=product_id_1_limit,
                   product_id=product_id,
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
    product_html_table_fmt = '''
                            <tr>
                                <th></th>
                                <th></th>
                                <th colspan="10" style="text-align: center;">关键指标</th>
                                <th colspan="4" style="text-align: center;">乘客指标</th>
                                <th colspan="10" style="text-align: center;">司机指标</th>
                                <th colspan="4" style="text-align: center;">体验指标</th>
                                <th colspan="6" style="text-align: center;">财务指标</th>
                            </tr>
                            <tr>
                                <th>日期</th>
                                <th>城市</th>
                                <!--关键指标-->
                                <th>下单数</th>
                                <th>下单数（近四周均值）</th>
                                <th>有效下单数</th>
                                <th>支付完单数</th>
                                <th>完单数</th>
                                <th>完单数（近四周均值）</th>
                                <th>完单率</th>
                                <th>完单率（近四周均值）</th>
                                <th>业务完单占比</th>
                                <th>城市完单占比</th>
                                <!--乘客指标-->
                                <th>完单乘客数</th>
                                <th>首次完单乘客数</th>
                                <th>当日注册乘客完单数</th>
                                <th>线上支付乘客占比</th>
                                <!--司机指标-->
                                <th>审核通过司机数</th>
                                <th>在线司机数</th>
                                <th>接单司机数</th>
                                <th>完单司机数</th>
                                <th>人均完单数</th>
                                <th>人均在线时长（时）</th>
                                <th>计费时长占比</th>
                                <th>人均推送订单数</th>
                                <th>TPH</th>
                                <th>IPH</th>
                                <!--体验指标-->
                                <th>平均应答时长（秒）</th>
                                <th>平均接驾时长（秒）</th>
                                <th>平均计费时长（秒）</th>
                                <th>平均送驾距离（米）</th>                         
                                <!--财务指标-->
                                <th>GMV</th>
                                <th>当日注册且完单乘客GMV</th>
                                <th>B端补贴率</th>
                                <th>C端补贴率</th>
                                <th>单均应付</th>
                                <th>单均实付</th>
                            </tr>
        '''

    product_html_table_fmt_otrike = '''
                                <tr>
                                    <th></th>
                                    <th></th>
                                    <th colspan="10" style="text-align: center;">关键指标</th>
                                    <th colspan="5" style="text-align: center;">乘客指标</th>
                                    <th colspan="10" style="text-align: center;">司机指标</th>
                                    <th colspan="4" style="text-align: center;">体验指标</th>
                                    <th colspan="6" style="text-align: center;">财务指标</th>
                                </tr>
                                <tr>
                                    <th>日期</th>
                                    <th>城市</th>
                                    <!--关键指标-->
                                    <th>下单数</th>
                                    <th>下单数（近四周均值）</th>
                                    <th>有效下单数</th>
                                    <th>支付完单数</th>
                                    <th>完单数</th>
                                    <th>完单数（近四周均值）</th>
                                    <th>完单率</th>
                                    <th>完单率（近四周均值）</th>
                                    <th>业务完单占比</th>
                                    <th>城市完单占比</th>
                                    <!--乘客指标-->
                                    <th>完单乘客数</th>
                                    <th>当日注册乘客下单数</th>
                                    <th>当日注册乘客完单数</th>
                                    <th>单均乘客数</th>
                                    <th>线上支付乘客占比</th>
                                    <!--司机指标-->
                                    <th>审核通过司机数</th>
                                    <th>在线司机数</th>
                                    <th>接单司机数</th>
                                    <th>完单司机数</th>
                                    <th>人均完单数</th>
                                    <th>人均在线时长（时）</th>
                                    <th>计费时长占比</th>
                                    <th>人均推送订单数</th>
                                    <th>TPH</th>
                                    <th>IPH</th>
                                    <!--体验指标-->
                                    <th>平均应答时长（秒）</th>
                                    <th>平均接驾时长（秒）</th>
                                    <th>平均计费时长（秒）</th>
                                    <th>平均送驾距离（米）</th>                         
                                    <!--财务指标-->
                                    <th>GMV</th>
                                    <th>当日注册且完单乘客GMV</th>
                                    <th>B端补贴率</th>
                                    <th>C端补贴率</th>
                                    <th>单均应付</th>
                                    <th>单均实付</th>
                                </tr>
            '''

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
                            <th>当日注册且完单乘客GMV</th>
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
                <table width="100%" class="table">
                <caption>
                    <h3>专车分城市指标</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {product_html_table_fmt}
                    </thead>
                    <tbody>
                    {direct_rows}
                    </tbody>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>快车分城市指标</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {product_html_table_fmt}
                    </thead>
                    <tbody>
                    {street_rows}
                    </tbody>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>OTrike分城市指标</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {product_html_table_fmt_otrike}
                    </thead>
                    <tbody>
                    {otrike_rows}
                    </tbody>
                </table> 
        </body>
        </html>
        '''
    result=get_all_data_row(ds)
    #print (result)
    #print (len(result))
    rows=result[0]
    all_completed_num=result[1]
    print (all_completed_num)
    html = html_fmt.format(rows=rows,
                           product_html_table_fmt=product_html_table_fmt,
                           product_html_table_fmt_otrike=product_html_table_fmt_otrike,
                           direct_rows=get_product_rows(ds,all_completed_num,1),
                           street_rows=get_product_rows(ds,all_completed_num,2),
                           otrike_rows=get_product_rows(ds,all_completed_num,3)
                           ) #

    logging.info("==============")
    #logging.info(html)

    email_to = Variable.get("oride_global_operate_report_receivers").split()
    #email_to = ['lili.chen@opay-inc.com']
    # send mail

    email_subject = 'oride全局运营日报_DW数仓模型构建_{}'.format(ds)
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
