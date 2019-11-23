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
from airflow.sensors import UFileSensor

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
    schedule_interval="00 02 * * *",
    default_args=args)

global_table_names = [
    'oride_dw.app_oride_global_operate_report_d',
    'oride_dw_ods.ods_sqoop_base_data_city_conf_df',
]

# 依赖前一天分区
app_oride_global_operate_report_d_task = UFileSensor(
    task_id='app_oride_global_operate_report_d_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/app_oride_global_operate_report_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
app_oride_global_operate_report_multi_d_task = UFileSensor(
    task_id='app_oride_global_operate_report_multi_d_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/app_oride_global_operate_report_multi_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
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
                            <!--系统-->
                            <td>{}</td>
                    '''
    sql = '''
                select dt,
                from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
                nvl(ride_order_cnt,0) as ride_order_cnt, --当日下单数
                nvl(order_cnt_lfw,0) as order_cnt_lfw, --下单数（近四周同期均值）dt>'2019-10-10'有近四周数据
                nvl(valid_ord_cnt,0) as valid_ord_cnt,  --当日有效下单量
                nvl(finish_pay,0) as finish_pay, --当日支付完单数
                nvl(finish_order_cnt,0) as finish_order_cnt, --当日完单量
                nvl(finish_order_cnt_lfw,0) as finish_order_cnt_lfw, --完单数（近四周同期均值）
                concat(cast(nvl(round(finish_order_cnt*100/ride_order_cnt,1),0) as string),'%') as finish_order_rate, --完单率
                concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) as string),'%') as finish_order_rate_lfw, --完单率（近四周）
                nvl(beckoning_num,0) as beckoning_num, --当日招手停完单数
                nvl(new_users,0) as new_users, --当日注册乘客数
                nvl(act_users,0) as act_users, --当日活跃乘客数
                nvl(ord_users,0) as ord_users, --当日下单乘客数
                nvl(first_finished_users,0) as first_finished_users,  --当日首次完单乘客数
                nvl(old_finished_users,0) as old_finished_users,  --当日完单老客数
                nvl(new_user_ord_cnt,0) as new_user_ord_cnt, --当日注册乘客下单量
                nvl(new_user_finished_cnt,0) as new_user_finished_cnt, --当日注册乘客完单量
                concat(cast(nvl(round(online_paid_users*100/paid_users,1),0) as string),'%') as online_paid_users_rate,  --当日线上支付乘客占比
                nvl(td_audit_finish_driver_num,0) as td_audit_finish_driver_num, --当日审核通过司机数
                nvl(td_online_driver_num,0) as td_online_driver_num, --当日在线司机数
                nvl(td_request_driver_num_inSimulRing,0) as td_online_driver_num, --当日接单司机数
                nvl(td_finish_order_driver_num_inSimulRing,0) as td_finish_order_driver_num_inSimulRing, --当日完单司机数
                cast(price as bigint) as gmv,  --订单应付总额,状态4，5
                cast(new_user_gmv as bigint) as new_user_gmv, -- 当日新注册乘客完单gmv，状态4，5
                concat(cast(nvl(round((recharge_amount+reward_amount)*100/price,1),0) as string),'%') as b_subsidy_rate,  --b端补贴率
                concat(cast(nvl(round((price-pay_amount)*100/price,1),0) as string),'%') as c_subsidy_rate, --c端补贴率【gmv状态4，5；实付金额状态5】
                --cast(user_recharge_succ_balance as bigint) as user_recharge_succ_balance, --每日用户充值真实金额
                --recharge_users, --每日充值客户数
                nvl(map_request_num,0) as map_request_num,  --地图调用次数
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
            week = data[1]
            print (week)
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)
            all_completed_num_nobeckon = data_list[0][6] - data_list[0][10]
            all_completed_num = data_list[0][6]

    return row_html, all_completed_num_nobeckon,all_completed_num


def get_product_rows(ds, all_completed_num_nobeckon, product_id):
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
        row_fmt = row_fmt1
    else:
        row_fmt = row_fmt2

    sql = '''
            SELECT t1.dt,
                 t1.city_id,
                 if(t1.city_id=-10000,'All',t2.name) AS city_name,
                 nvl(t1.ride_order_cnt,0) as ride_order_cnt, --当日下单数
                 nvl(if(dt>'2019-11-21' and t1.product_id<>4,order_cnt_lfw,if(dt>'2019-12-17',order_cnt_lfw,'-')),0) AS order_cnt_lfw, --近四周同期下单数据 dt>'2019-11-21'专快otrike有近四周数据
                 nvl(t1.valid_ord_cnt,0) as valid_ord_cnt, --有效下单量
                 nvl(t1.finish_pay,0) as finish_pay, --当日支付完单数
                 nvl(t1.finish_order_cnt,0) as finish_order_cnt, --当日完单量
                 nvl(if(dt>'2019-11-21' and t1.product_id<>4,finish_order_cnt_lfw,if(dt>'2019-12-17',finish_order_cnt_lfw,'-')),0) AS finish_order_cnt_lfw, --完单数（近四周同期均值）
                 concat(cast(nvl(round(t1.finish_order_cnt*100/t1.ride_order_cnt,1),0) AS string),'%') AS finish_order_rate, --完单率
                 if(dt>'2019-11-21' and t1.product_id<>4,concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) as string),'%'),if(dt>'2019-12-17',concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) as string),'%'),'-')) AS finish_order_rate_lfw, --完单率（近四周）
                 concat(cast(nvl(round(if(t1.city_id=-10000,t1.finish_order_cnt*2*100/t1.total,t1.finish_order_cnt*100/t1.city_total),1),0) AS string),'%') AS product_finish_order_rate,--业务完单占比
                 concat(cast(nvl(round(if(t1.city_id=-10000,t1.finish_order_cnt*100/t1.finish_order_cnt,t1.finish_order_cnt*2*100/sum(t1.finish_order_cnt) over(partition BY t1.product_id)),1),0) AS string),'%') AS city_finish_order_rate, --城市完单
                 nvl(t1.finished_users,0) as finished_users,--当日完单乘客数
                 nvl(if(product_id=3,t1.new_user_ord_cnt,t1.first_finished_users),0) as passenger_indictor_2, --1,2当日首次完单乘客数;3当日注册乘客下单数
                 nvl(t1.new_user_finished_cnt,0) as new_user_finished_cnt, --当日新注册乘客完单数
                 if(product_id=3,nvl(round(t1.pax_num/t1.finish_order_cnt,0),0),0) as passenger_indictor_4,
                 concat(cast(nvl(round(t1.online_paid_users*100/t1.paid_users,1),0) AS string),'%') AS online_paid_user_rate, --当日线上支付乘客占比
                 nvl(t1.td_audit_finish_driver_num,0) as td_audit_finish_driver_num, --当日审核通过司机数
                 nvl(t1.td_online_driver_num,0) as td_online_driver_num, --当日在线司机数
                 nvl(t1.td_request_driver_num,0) as td_request_driver_num, --当日接单司机数(不包含同时呼叫)
                 nvl(t1.td_finish_order_driver_num,0) as td_finish_order_driver_num, --当日完单司机数(不包含同时呼叫)
                 nvl(round(t1.finish_order_cnt/t1.td_finish_order_driver_num,0),0) AS avg_finish_order_cnt, --人均完单数
                 nvl(round(t1.finish_driver_online_dur/t1.td_finish_order_driver_num_insimulring/3600,1),0) AS avg_driver_online_dur, --人均在线时长
                 concat(cast(nvl(round(t1.driver_billing_dur*100/t1.finish_driver_online_dur,1),0) AS string),'%') AS billing_dur_rate, --计费时长占比
                 nvl(round(t1.driver_pushed_order_cnt/t1.td_push_accpet_show_driver_num,0),0) AS avg_pushed_order_cnt, --人均推送订单数
                 nvl(round(t1.finish_order_cnt_inSimulRing/t1.finish_driver_online_dur*3600,1),0) AS TPH, --分子完单量用（包含同时呼叫的）
                 '-' as IPH,
                 --nvl(round(t1.iph_fenzi_inSimulRing*3600/t1.finish_driver_online_dur,1),0) AS IPH, --分子（包含同时呼叫）
                 if(t1.finish_order_cnt>=10000,concat(cast(nvl(round(t1.bad_feedback_finish_ord_cnt*10000/t1.finish_order_cnt,0),0) AS string),'‱'),'-') as bad_feedback_finish_rate, --万单完单差评率
                 nvl(cast(round(t1.finish_take_order_dur/t1.finish_order_cnt,0) as bigint),0) AS avg_take_order_dur,--平均应答时长
                 nvl(cast(round(t1.finish_pick_up_dur/t1.finish_order_cnt,0) as bigint),0) AS avg_pick_up_dur, --平均接驾时长
                 nvl(cast(round(t1.billing_order_dur/t1.finish_order_cnt,0) as bigint),0) AS avg_billing_order_dur,--平均计费时长
                 nvl(cast(round(t1.finish_order_onride_dis/t1.finish_order_cnt,0) as bigint),0) AS avg_order_onride_dis,--平均送驾距离
                 nvl(cast(t1.price as bigint),0) AS gmv,
                 nvl(cast(t1.new_user_gmv as bigint),0) as new_user_gmv, --当日注册乘客完单gmv
                concat(cast(nvl(round((t1.recharge_amount+t1.reward_amount)*100/t1.price,1),0) AS string),'%') AS b_subsidy_rate, --b端补贴率
                concat(cast(nvl(round((t1.price-t1.pay_amount)*100/t1.price,1),0) AS string),'%') AS c_subsidy_rate, --c端补贴率【gmv状态4，5；实付金额状态5】
                nvl(round(t1.pay_price/t1.finish_pay,1),0) AS avg_pay_price, --单均应付
                nvl(round(t1.pay_amount/t1.finish_pay,1),0) AS avg_pay_amount --单均实付
                from (SELECT sum(finish_order_cnt) over(partition BY city_id) AS city_total,
                             sum(finish_order_cnt) over(partition BY dt) AS total,
                             *
                      FROM oride_dw.app_oride_global_operate_report_d WHERE dt='{dt}' and product_id not in(0,99,-10000)) t1
                   left JOIN
                  (SELECT id,
                          name
                   FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df
                   WHERE dt='{dt}') t2 ON t1.city_id=t2.id
                   where t1.country_code='nal' --某个业务线汇总及城市明细
                     AND t1.product_id={product_id}
                ORDER BY t1.city_id ASC
        '''.format(dt=ds,
                   all_completed_num_nobeckon=all_completed_num_nobeckon,
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


def get_all_product_rows(ds, all_completed_num):
    tr_fmt = '''
               <tr>{row}</tr>
            '''
    row_fmt = '''
                            <td>{}</td>
                            <!--{}-->
                            <td>{}</td>
                            <!--天气指标-->
                            <td>{}</td>
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
                            <!--各业务完单占比-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td> 
                            <td>{}</td> 
                    '''
    sql = '''SELECT t1.dt,
                     t1.city_id,
                     if(t1.city_id=-10000,'All',cit.city_name) AS city_name,
                     if(t1.city_id=-10000,'-',nvl(cit.weather,'-')) AS weather, --当日该城市天气
                     concat(cast(nvl(round(nvl(t1.wet_ord_cnt,0)*100/nvl(t1.ride_order_cnt,0),1),0) AS string),'%') AS wet_order_rate, --湿单占比
                     nvl(t1.ride_order_cnt,0) AS ride_order_cnt, --当日下单数
                     if(t1.dt>'2019-11-21',order_cnt_lfw,'-') AS order_cnt_lfw, --近四周同期下单数据
                     nvl(t1.valid_ord_cnt,0) AS valid_ord_cnt, --有效下单量
                     nvl(t1.finish_pay,0) AS finish_pay, --当日支付完单数
                     nvl(t1.finish_order_cnt,0) AS finish_order_cnt, --当日完单量
                     if(t1.dt>'2019-11-21',finish_order_cnt_lfw,'-') AS finish_order_cnt_lfw, --完单数（近四周同期均值）
                     concat(cast(nvl(round(t1.finish_order_cnt*100/t1.ride_order_cnt,1),0) AS string),'%') AS finish_order_rate, --完单率
                     if(t1.dt>'2019-11-21',concat(cast(nvl(round(finish_order_cnt_lfw*100/order_cnt_lfw,1),0) AS string),'%'),'-') AS finish_order_rate_lfw, --完单率（近四周）
                     concat(cast(nvl(round(nvl(t1.finish_order_cnt,0)*100/t6.finish_order_cnt,1),0) AS string),'%') AS city_fininsh_ord_rate, --城市完单占比
                     nvl(t1.ord_users,0) AS ord_users, --当日下单乘客数
                     nvl(t1.finished_users,0) AS finished_users,--当日完单乘客数
                     concat(cast(nvl(round(t1.online_paid_users*100/t1.paid_users,1),0) AS string),'%') AS online_paid_user_rate, --当日线上支付乘客占比
                    concat(cast(nvl(round(nvl(t2.finish_order_cnt,0)*100/nvl(t1.finish_order_cnt,0),1),0) AS string),'%') AS direct_finish_rate, --专车完单占比
                    concat(cast(nvl(round(nvl(t3.finish_order_cnt,0)*100/nvl(t1.finish_order_cnt,0),1),0) AS string),'%') AS street_finish_rate, --快车完单占比
                    concat(cast(nvl(round(nvl(t4.finish_order_cnt,0)*100/nvl(t1.finish_order_cnt,0),1),0) AS string),'%') AS otrike_finish_rate, --otrike完单占比  
                    concat(cast(nvl(round(nvl(t5.finish_order_cnt,0)*100/nvl(t1.finish_order_cnt,0),1),0) AS string),'%') AS ocar_finish_rate --ocar完单占比      
                    FROM
                      (SELECT *
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=-10000) t1
                    LEFT JOIN
                      (SELECT city_id,
                              finish_order_cnt
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=1) t2 ON t1.city_id=t2.city_id
                    LEFT JOIN
                      (SELECT city_id,
                              finish_order_cnt
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=2) t3 ON t1.city_id=t3.city_id
                    LEFT JOIN
                      (SELECT city_id,
                              finish_order_cnt
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=3) t4 ON t1.city_id=t4.city_id
                    LEFT JOIN
                      (SELECT city_id,
                              finish_order_cnt
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=4) t5 ON t1.city_id=t5.city_id
                    LEFT JOIN
                      (SELECT city_id,
                              finish_order_cnt
                       FROM oride_dw.app_oride_global_operate_report_multi_d
                       WHERE dt='{dt}'
                         AND driver_serv_type=-10000 and city_id=-10000) t6 ON 1=1
                    LEFT JOIN
                      (SELECT *
                       FROM oride_dw.dim_oride_city
                       WHERE dt='{dt}') cit ON t1.city_id=cit.city_id
                    ORDER BY t1.city_id ASC
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
    # logging.info("receivers:%s" % Variable.get("oride_global_operate_report_receivers"))

    all_product_city_total_fmt = '''
                            <tr>
                                <th></th>
                                <th></th>
                                <th colspan="2" style="text-align: center;">天气指标</th>
                                <th colspan="9" style="text-align: center;">关键指标</th>
                                <th colspan="3" style="text-align: center;">乘客指标</th>
                                <th colspan="4" style="text-align: center;">各业务完单占比</th>
                            </tr>
                            <tr>
                                <th>日期</th>
                                <th>城市</th>
                                <!--天气指标-->
                                <th>天气</th>
                                <th>湿单占比</th>
                                <!--关键指标-->
                                <th>下单数</th>
                                <th>下单数（近四周均值）</th>
                                <th>有效下单数</th>
                                <th>支付完单数</th>
                                <th>完单数</th>
                                <th>完单数（近四周均值）</th>
                                <th>完单率</th>
                                <th>完单率（近四周均值）</th>
                                <th>城市完单占比</th>
                                <!--乘客指标-->
                                <th>下单乘客数</th>
                                <th>完单乘客数</th>
                                <th>线上支付乘客占比</th>
                                <!--各业务完单占比-->
                                <th>专车</th>
                                <th>快车</th>
                                <th>Otrike</th>
                                <th>Ocar</th>
                            </tr>
    '''
    product_html_table_fmt = '''
                            <tr>
                                <th></th>
                                <th></th>
                                <th colspan="10" style="text-align: center;">关键指标</th>
                                <th colspan="4" style="text-align: center;">乘客指标</th>
                                <th colspan="11" style="text-align: center;">司机指标</th>
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
                                <th>万单差评率</th>
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
                                    <th colspan="11" style="text-align: center;">司机指标</th>
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
                                    <th>万单差评率</th>
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
                            <th colspan="5" style="text-align: center;">财务</th>
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
                    <h3>多业务城市汇总</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {all_product_city_total_fmt}
                    </thead>
                    <tbody>
                    {all_product_city_total_rows}
                    </tbody>
                </table>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>专车分城市指标(不包含同时呼叫)</h3>
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
                    <h3>快车分城市指标(不包含同时呼叫)</h3>
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
                <table width="100%" class="table">
                <caption>
                    <h3>Ocar分城市指标(不包含同时呼叫)</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {product_html_table_fmt}
                    </thead>
                    <tbody>
                    {ocar_rows}
                    </tbody>
                </table>
        </body>
        </html>
        '''
    result = get_all_data_row(ds)
    # print (result)
    # print (len(result))
    rows = result[0]
    all_completed_num_nobeckon=result[1]
    all_completed_num=result[2]
    print (all_completed_num_nobeckon,all_completed_num)
    html = html_fmt.format(rows=rows,
                           all_product_city_total_fmt=all_product_city_total_fmt,
                           product_html_table_fmt=product_html_table_fmt,
                           product_html_table_fmt_otrike=product_html_table_fmt_otrike,
                           direct_rows=get_product_rows(ds, all_completed_num_nobeckon, 1),
                           street_rows=get_product_rows(ds, all_completed_num_nobeckon, 2),
                           otrike_rows=get_product_rows(ds, all_completed_num_nobeckon, 3),
                           ocar_rows=get_product_rows(ds, all_completed_num_nobeckon, 4),
                           all_product_city_total_rows=get_all_product_rows(ds, all_completed_num)
                           )  #

    logging.info("==============")
    # logging.info(html)

    email_to = Variable.get("oride_global_operate_report_receivers").split()

    #email_to = ['lili.chen@opay-inc.com']
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

app_oride_global_operate_report_d_task >> app_oride_global_operate_report_multi_d_task >> send_report
