# -*- coding: utf-8 -*-
"""
调度算法效果监控指标新版2019-08-02
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

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_order_dispatch_d',
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
dependence_app_oride_order_pushed_d = UFileSensor(
    task_id='dependence_app_oride_order_pushed_d',
    filepath='{hdfs_path_str}/type=serv/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/app_oride_order_pushed_d",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_app_oride_driver_base_d = UFileSensor(
    task_id='dependence_app_oride_driver_base_d',
    filepath='{hdfs_path_str}/type=serv/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/app_oride_driver_base_d",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_app_oride_order_base_d = UFileSensor(
    task_id='dependence_app_oride_order_base_d',
    filepath='{hdfs_path_str}/type=serv/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/app_oride_order_base_d",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)
"""
##-----end-----##
"""

hive_table = 'oride_dw.app_oride_order_dispatch_di'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {hive_table} (
            city_id                             bigint          comment '城市ID',
            city_name                           string          comment '城市名称',
            product_id                          bigint          comment '业务线',
            broadcast_distance                  decimal(38,2)   comment '平均播单距离',
            pickup_distance_done                decimal(38,2)   comment '平均接驾距离(完单)',
            pickup_distance_take                decimal(38,2)   comment '平均接驾距离(应答)',
            dispatch_obey_rate                  decimal(38,2)   comment '调度服从率',
            take_rate                           decimal(38,2)   comment '应答率',
            take_order_dur_done                 decimal(38,2)   comment '平均应答时长(完单)',
            take_order_dur_take                 decimal(38,2)   comment '平均应答时长(应答)',
            passanger_after_cancel_rate         decimal(38,2)   comment '乘客应答后取消率',
            driver_after_cancel_rate            decimal(38,2)   comment '司机应答后取消率',
            tph_macro                           decimal(38,2)   comment 'tph', 
            online_range                        decimal(38,2)   comment '平均在线时长',
            billing_order_dur_rate              decimal(38,2)   comment '计费时长占比', 
            ride_order_cnt                      bigint          comment '下单数',
            broadcast_cnt                       bigint          comment '播单数',
            succ_broadcast_cnt                  bigint          comment '成功播单数',
            broadcast_rate                      decimal(38,2)   comment '播单率',
            request_order_cnt                   bigint          comment '接单数',
            request_rate                        decimal(38,2)   comment '接单率',
            dispatch_request_rate               decimal(38,2)   comment '调度接单率',
            finish_order_cnt                    bigint          comment '完单数',
            finish_rate                         decimal(38,2)   comment '完单率',
            dispatch_finish_rate                decimal(38,2)   comment '调度完单率',
            tph_order                           decimal(38,2)   comment 'tph',
            send_done_distance                  decimal(38,2)   comment '平均送驾距离', 
            pick_up_order_dur                   decimal(38,2)   comment '平均接驾时长',
            billing_order_dur                   decimal(38,2)   comment '平均计费时长',
            pay_order_dur                       decimal(38,2)   comment '平均支付时长',
            drivers_online                      bigint          comment '在线司机数',
            drivers_take                        bigint          comment '接单司机数',
            drivers_finished                    bigint          comment '完单司机数',
            finished_per_driver                 decimal(38,2)   comment '人均完单数',
            push_per_driver                     decimal(38,2)   comment '人均推送订单数',
            take_per_driver                     decimal(38,2)   comment '人均应答订单数',
            driver_obey_rate                    decimal(38,2)   comment '司机服从率',
            driver_online_dur                   decimal(38,2)   comment '人均在线时长',
            driver_service_dur                  decimal(38,2)   comment '人均服务时长',
            driver_iph_done                     decimal(38,2)   comment '完单司机IPH', 
            driver_salary_done                  decimal(38,2)   comment '完单司机日薪', 
            push_arrive_rate                    decimal(38,2)   comment '播报到达率',
            driver_push_cnt                     decimal(38,2)   comment '人均推单次数',
            driver_pushed_cnt                   decimal(38,2)   comment '平均推送司机数',
            cancel_order_cnt                    bigint          comment '取消订单数',
            sys_cancel_order_cnt                bigint          comment '系统取消订单数',
            sys_cancel_rate                     decimal(38,2)   comment '系统取消率',
            passanger_before_cancel_order_cnt   bigint          comment '乘客应答前取消数',
            passanger_before_cancel_order_rate  decimal(38,2)   comment '乘客应答前取消率',
            after_cancel_order_cnt              bigint          comment '应答后取消数',
            passanger_after_cancel_order_cnt    bigint          comment '乘客应答后取消数',
            passanger_after_cancel_order_dur    decimal(38,2)   comment '乘客应答后取消平均时长（分）',
            passanger_after_cancel_order_rate   decimal(38,2)   comment '乘客应答后取消率',
            passanger_cancel_dis                decimal(38,2)   comment '乘客取消订单平均接驾距离',
            driver_after_cancel_order_cnt       bigint          comment '司机应答后取消数',
            driver_after_cancel_order_dur       decimal(38,2)   comment '司机应答后取消平均时长（分）',
            driver_after_cancel_order_rate      decimal(38,2)   comment '司机应答后取消率',
            driver_take_num                     bigint          comment '司机应答总次数',
            take_num_per_driver                 bigint          comment '司机人均应答次数',
            valid_orders                        bigint          comment '有效下单数',
            valid_orders_finished               decimal(38,2)   comment '有效完单率'
        )
        PARTITIONED BY (
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
            )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_order_dispatch_d'
    '''.format(hive_table=hive_table),
    schema='oride_dw',
    dag=dag
)


def drop_partions(*op_args, **op_kwargs):
    dt = op_kwargs['ds']
    cursor = get_hive_cursor()
    sql = '''
        show partitions {hive_table}
    '''.format(hive_table=hive_table)
    cursor.execute(sql)
    res = cursor.fetchall()
    logging.info(res)
    for partition in res:
        prt, = partition
        matched = re.search(r'country_code=(?P<cc>\w+)/dt=(?P<dy>.*)$', prt)
        cc = matched.groupdict().get('cc', 'nal')
        dy = matched.groupdict().get('dy', '')
        if dy == dt:
            hql = '''
                ALTER TABLE {hive_table} DROP IF EXISTS PARTITION (country_code='{cc}', dt = '{dt}')
            '''.format(cc=cc, dt=dt, hive_table=hive_table)
            logging.info(hql)
            cursor.execute(hql)


"""
drop_partitons_from_table = PythonOperator(
    task_id='drop_partitons_from_table',
    python_callable=drop_partions,
    provide_context=True,
    dag=dag
)
"""

insert_result_to_hive = HiveOperator(
    task_id='insert_result_to_hive',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        WITH
        --推单数据 
        app_oride_order_pushed_d AS 
        (
            SELECT 
                * 
            FROM oride_dw.app_oride_order_pushed_d 
            WHERE dt = '{pt}'
        ), 
        --司机数据
        app_oride_driver_base_d AS 
        (
            SELECT 
                * 
            FROM oride_dw.app_oride_driver_base_d 
            WHERE dt = '{pt}' 
        ),
        --订单数据
        app_oride_order_base_d AS 
        (
            SELECT 
                * 
            FROM oride_dw.app_oride_order_base_d 
            WHERE dt = '{pt}'
        )

        ---写入结果表
        INSERT OVERWRITE TABLE {hive_table} PARTITION (country_code, dt) 
        SELECT 
            --城市ID
            NVL(a.city_id, NVL(b.city_id, c.city_id)), 
            --城市名
            '', 
            --业务类型
            NVL(a.product_id, NVL(b.product_id, c.product_id)), 
            --平均播单距离
            CASE WHEN a.avg_pushed_distance IS NULL THEN 0 ELSE a.avg_pushed_distance END, 
            --平均接驾距离(完单)
            CASE WHEN a.avg_picked_distance_done IS NULL THEN 0 ELSE a.avg_picked_distance_done END, 
            --平均接驾距离(应答)
            CASE WHEN a.avg_picked_distance_take IS NULL THEN 0 ELSE a.avg_picked_distance_take END, 
            --调度服从率
            ROUND(
                IF(IF(a.drivers_pushed_success>0,
                        NVL(a.drivers_count_pushed,0)/a.drivers_pushed_success,
                    0)>0, 
                    IF(b.driver_avg_clicked IS NULL,0,b.driver_avg_clicked)/(a.drivers_count_pushed/a.drivers_pushed_success), 
                    0
                ), 
            2), 
            --应答率
            ROUND(IF(c.orders>0, 
                NVL(c.orders_taked,0)/c.orders, 
                0), 
            2), 
            --平均应答时长(完单分)                
            ROUND(NVL(c.avg_take_range,0)/60, 2), 
            --平均应答时长(应答分)
            ROUND(NVL(c.avg_take_range_tk,0)/60, 2),                                                                                  
            --乘客应答后取消率       
            ROUND(IF(c.orders>0, 
                NVL(c.cancel_af_take_us,0)/c.orders, 
                0), 
            2), 
            --司机应答后取消率                                                        
            ROUND(IF(c.orders>0, 
                NVL(c.cancel_af_take_dr,0)/c.orders, 
                0), 
            2), 
            --tph    
            ROUND(IF((NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))>0, 
                NVL(c.orders_f,0)*3600/(NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0)), 
                0), 
            2), 
            --平均在线时长(时)     
            ROUND(IF(b.drivers_order_finished>0, 
                (NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))/b.drivers_order_finished/3600, 
                0),
            2), 
            --计费时长占比
            ROUND(IF((NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))>0, 
                NVL(c.billing_range,0)/(NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0)), 
                0), 
            2), 
            --下单数    
            NVL(c.orders, 0), 
            --播单数
            NVL(a.orders_pushed, 0), 
            --成功播单数
            NVL(a.orders_pushed_success, 0), 
            --播单率
            ROUND(IF(c.orders>0, 
                NVL(a.orders_pushed_success,0)/c.orders, 
                0), 
            2), 
            --'接单数',
            NVL(c.orders_taked, 0), 
            --'接单率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.orders_taked,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'调度接单率',
            ROUND(IF(NVL(a.orders_pushed_success,0)>0, 
                NVL(c.orders_taked,0)/NVL(a.orders_pushed_success,0), 
                0), 
            2), 
            --'完单数',
            NVL(c.orders_f, 0), 
            --'完单率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.orders_f,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'调度完单率',
            ROUND(IF(NVL(a.orders_pushed,0)>0, 
                NVL(c.orders_f,0)/NVL(a.orders_pushed,0), 
                0), 
            2), 
            --'tph',
            ROUND(IF((NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))>0, 
                NVL(c.orders_f,0)*3600/(NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0)), 
                0), 
            2), 
            --'平均送驾距离', 
            ROUND(IF(NVL(c.orders_f,0)>0, 
                IF(c.send_dis IS NULL,0,c.send_dis)/NVL(c.orders_f,0), 
                0), 
            2), 
            --'平均接驾时长'(分), 
            ROUND(IF(NVL(c.orders_f,0)>0, 
                NVL(c.pick_range,0)/NVL(c.orders_f,0)/60, 
                0), 
            2), 
            --'平均计费时长'(分),
            ROUND(IF(NVL(c.orders_f,0)>0, 
                NVL(c.billing_range,0)/NVL(c.orders_f,0)/60, 
                0), 
            2), 
            --'平均支付时长'(分),
            ROUND(IF(NVL(c.payed_orders,0)>0, 
                NVL(c.pay_range,0)/NVL(c.payed_orders,0)/60, 
                0), 
            2), 
            --'在线司机数',
            NVL(b.drvers_online, 0), 
            --'接单司机数',
            NVL(b.drivers_clicked, 0), 
            --'完单司机数', 
            NVL(b.drivers_order_finished, 0), 
            --'人均完单数',
            ROUND(IF(NVL(b.drivers_order_finished,0)>0, 
                NVL(c.orders_f,0)/NVL(b.drivers_order_finished,0), 
                0), 
            2), 
            --'人均推送订单数',
            ROUND(IF(NVL(b.drivers_showed,0)>0, 
                NVL(b.orders_showed,0)/NVL(b.drivers_showed,0), 
                0), 
            2), 
            --'人均应答订单数',
            ROUND(IF(NVL(b.drivers_clicked,0)>0, 
                NVL(b.orders_clicked_all,0)/NVL(b.drivers_clicked,0), 
                0), 
            2), 
            --'司机服从率',            
            ROUND(IF(
                NVL(b.drivers_showed,0)>0 AND NVL(b.orders_showed,0)/NVL(b.drivers_showed,0)>0 AND NVL(b.drivers_clicked,0)>0, 
                    (NVL(b.orders_clicked_all,0)/NVL(b.drivers_clicked,0))/(NVL(b.orders_showed,0)/NVL(b.drivers_showed,0)), 
                0), 
            2), 
            --'人均在线时长'(时),
            ROUND(IF(b.drivers_order_finished>0, 
                (NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))/b.drivers_order_finished/3600, 
                0),
            2), 
            --'平均服务时长',    
            ROUND(IF(NVL(c.orders_f,0)>0, 
                NVL(b.driver_do_range,0)/NVL(c.orders_f,0), 
                0), 
            2), 
            --'完单司机IPH', 
            ROUND(IF((NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0))>0, 
                (IF(b.payed_online IS NULL,0,b.payed_online)+IF(b.payed_offline IS NULL,0,b.payed_offline)+IF(b.driver_reward IS NULL,0,b.driver_reward))/(NVL(b.driver_do_range,0)+NVL(b.driver_free_range,0)), 
                0), 
            2), 
            --'完单司机日薪', 
            ROUND(IF(NVL(b.drivers_order_finished,0)>0, 
                (IF(b.payed_online IS NULL,0,b.payed_online)+IF(b.payed_offline IS NULL,0,b.payed_offline)+IF(b.driver_reward IS NULL,0,b.driver_reward))/NVL(b.drivers_order_finished,0), 
                0), 
            2), 
            --'播报到达率', 
            ROUND(IF(NVL(a.drivers_count_pushed_all,0)>0, 
                NVL(b.order_showed_count,0)/NVL(a.drivers_count_pushed_all,0), 
                0), 
            2), 
            --'人均推单次数',
            ROUND(IF(NVL(a.drivers_pushed_success,0)>0, 
                NVL(a.drivers_count_pushed,0)/NVL(a.drivers_pushed_success,0), 
                0), 
            2), 
            --'平均推送司机数',
            ROUND(IF(NVL(a.drivers_pushed_round,0)>0, 
                NVL(a.drivers_count_pushed,0)/NVL(a.drivers_pushed_round,0), 
                0), 
            2), 
            --'取消订单数',
            NVL(c.cancel_all, 0),
            --'系统取消订单数',
            NVL(c.cancel_sys, 0),  
            --'系统取消率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.cancel_sys,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'乘客应答前取消数',
            NVL(c.cancel_bf_take_us, 0),
            --'乘客应答前取消率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.cancel_bf_take_us,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'应答后取消数',    
            NVL(c.cancel_af_take_all, 0),  
            --'乘客应答后取消数',
            NVL(c.cancel_af_take_us, 0),  
            --'乘客应答后取消平均时长（分）',
            ROUND(IF(NVL(c.orders_taked,0)>0, 
                NVL(c.cancel_af_take_us_range,0)/NVL(c.orders_taked,0)/60, 
                0), 
            2), 
            --'乘客应答后取消率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.cancel_af_take_us,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'乘客取消订单平均接驾距离',    
            IF(a.avg_picked_distance_uscancel IS NULL, 0, a.avg_picked_distance_uscancel), 
            --'司机应答后取消数',
            NVL(c.cancel_af_take_dr, 0),  
            --'司机应答后取消平均时长（分）',
            ROUND(IF(NVL(c.orders_taked,0)>0, 
                NVL(c.cancel_af_take_dr_range,0)/NVL(c.orders_taked,0)/60, 
                0), 
            2), 
            --'司机应答后取消率',
            ROUND(IF(NVL(c.orders,0)>0, 
                NVL(c.cancel_af_take_dr,0)/NVL(c.orders,0), 
                0), 
            2), 
            --'司机应答总次数',
            NVL(b.driver_clicked_count, 0),
            --'司机人均应答次数',
            IF(b.driver_avg_clicked IS NULL, 0, b.driver_avg_clicked),  
            --'有效下单数',
            NVL(c.valid_orders, 0),
            --'有效完单率'
            ROUND(IF(NVL(c.valid_orders,0)>0, 
                NVL(c.orders_f,0)/NVL(c.valid_orders,0), 
                0), 
            2), 
            NVL(a.country_code, NVL(b.country_code, c.country_code)) AS country_code, 
            NVL(a.dt, NVL(b.dt, c.dt)) AS dt 
        FROM app_oride_order_pushed_d AS a 
        FULL OUTER JOIN app_oride_driver_base_d AS b ON b.country_code=a.country_code AND b.dt=a.dt AND b.city_id=a.city_id AND b.product_id=a.product_id 
        FULL OUTER JOIN app_oride_order_base_d AS c ON c.country_code=a.country_code AND c.dt=a.dt AND c.city_id=a.city_id AND c.product_id=a.product_id
    '''.format(
        pt='{{ ds }}', hive_table=hive_table
    ),
    schema='oride_dw',
    dag=dag
)

'''
校验分区代码
'''

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names": [hive_table],
        # 任务名称
        "task_name": "调度算法效果监控指标"
    },
    dag=dag
)

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


def sendDispatchMail(*op_args, **op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    # 全国指标
    hsql = '''
        SELECT 
            a.dt,
            a.city_id,
            'All',
            'all',
            a.broadcast_distance,                   --平均播单距离
            a.pickup_distance_done,                 --平均接驾距离完单
            a.pickup_distance_take,                 --平均接驾距离应答
            a.dispatch_obey_rate*100,                   --调度服从率
            a.take_rate*100,                            --应答率
            a.take_order_dur_done,                  --平均应答时长完单
            a.take_order_dur_take,                  --平均应答时长应答
            a.passanger_after_cancel_rate*100,          --乘客应答后取消率
            a.driver_after_cancel_rate*100,             --司机应答后取消率
            a.tph_macro,                            --TPH
            a.online_range,                         --人均在线时长
            a.billing_order_dur_rate*100                --计费时长占比
        FROM {table} AS a 
        WHERE a.dt >= date_sub('{pt}', 6) AND 
            a.dt <= '{pt}' AND 
            a.dt >= '2019-08-22' AND  
            a.city_id = 0 AND 
            a.product_id = -1 
        ORDER BY a.dt DESC 
    '''.format(table=hive_table, pt=dt)
    logging.info(hsql)
    cursor.execute(hsql)
    res = cursor.fetchall()
    header_all = '''
        <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 全国</th></tr>
    '''
    table_rows_all = ''
    for (day, city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in res:
        table_rows_all += table_rows.format(
            dt=day,
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
    # logging.info(table_rows_all)

    # 城市指标
    hsql = '''
        SELECT 
            a.dt,
            a.city_id,
            b.name,
            'all',
            a.broadcast_distance,                   --平均播单距离
            a.pickup_distance_done,                 --平均接驾距离完单
            a.pickup_distance_take,                 --平均接驾距离应答
            a.dispatch_obey_rate*100,                   --调度服从率
            a.take_rate*100,                            --应答率
            a.take_order_dur_done,                  --平均应答时长完单
            a.take_order_dur_take,                  --平均应答时长应答
            a.passanger_after_cancel_rate*100,          --乘客应答后取消率
            a.driver_after_cancel_rate*100,             --司机应答后取消率
            a.tph_macro,                            --TPH
            a.online_range,                         --人均在线时长
            a.billing_order_dur_rate*100                --计费时长占比
        FROM {table} AS a 
        JOIN (SELECT 
                id,
                name 
            FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df 
            WHERE dt = '{pt}' AND 
                id < 999000
            ) AS b 
        ON a.city_id = b.id 
        WHERE a.dt >= date_sub('{pt}', 2) AND 
            a.dt <= '{pt}' AND 
            a.dt >= '2019-08-22' AND 
            a.city_id > 0 AND 
            a.product_id = -1 
        ORDER BY a.city_id ASC, a.dt DESC   
    '''.format(table=hive_table, pt=dt)
    logging.info(hsql)
    cursor.execute(hsql)
    res = cursor.fetchall()
    header_city = '''
            <tr style="background-color:#bf9001"><th colspan="15">分城市宏观指标 - 全部业务线</th></tr>
        '''
    table_rows_city = ''
    for (day, city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in res:
        table_rows_city += table_rows.format(
            dt=day,
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
    # logging.info(table_rows_city)

    # green
    hsql = '''
        SELECT 
            a.dt,
            a.city_id,
            b.name,
            'Green',
            a.broadcast_distance,                   --平均播单距离
            a.pickup_distance_done,                 --平均接驾距离完单
            a.pickup_distance_take,                 --平均接驾距离应答
            a.dispatch_obey_rate*100,                   --调度服从率
            '-',                            --应答率
            a.take_order_dur_done,                  --平均应答时长完单
            a.take_order_dur_take,                  --平均应答时长应答
            '-',                                    --乘客应答后取消率
            '-',                                    --司机应答后取消率
            a.tph_macro,                            --TPH
            a.online_range,                         --人均在线时长
            a.billing_order_dur_rate*100                --计费时长占比
        FROM {table} AS a 
        JOIN (SELECT 
                id,
                name 
            FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df 
            WHERE dt = '{pt}' AND 
                id < 999000
            ) AS b 
        ON a.city_id = b.id 
        WHERE a.dt = '{pt}' AND   
            a.city_id > 0 AND 
            a.product_id = 1 
        ORDER BY a.city_id ASC, a.dt DESC 
    '''.format(table=hive_table, pt=dt)
    logging.info(hsql)
    cursor.execute(hsql)
    res = cursor.fetchall()
    header_green = '''
            <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Green</th></tr>
        '''
    table_rows_green = ''
    for (day, city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in res:
        table_rows_green += table_rows.format(
            dt=day,
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
    # logging.info(table_rows_green)

    # Street
    hsql = '''
        SELECT 
            a.dt,
            a.city_id,
            b.name,
            'Street',
            a.broadcast_distance,                   --平均播单距离
            a.pickup_distance_done,                 --平均接驾距离完单
            a.pickup_distance_take,                 --平均接驾距离应答
            a.dispatch_obey_rate*100,                   --调度服从率
            '-',                            --应答率
            a.take_order_dur_done,                  --平均应答时长完单
            a.take_order_dur_take,                  --平均应答时长应答
            '-',                                    --乘客应答后取消率
            '-',                                    --司机应答后取消率
            a.tph_macro,                            --TPH
            a.online_range,                         --人均在线时长
            a.billing_order_dur_rate*100                --计费时长占比
        FROM {table} AS a 
        JOIN (SELECT 
                id, 
                name 
            FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df 
            WHERE dt = '{pt}' AND 
                id < 999000
            ) AS b 
        ON a.city_id = b.id 
        WHERE a.dt = '{pt}' AND  
            a.city_id > 0 AND 
            a.product_id = 2 
        ORDER BY a.city_id ASC, a.dt DESC 
    '''.format(table=hive_table, pt=dt)
    logging.info(hsql)
    cursor.execute(hsql)
    res = cursor.fetchall()
    header_street = '''
            <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Street</th></tr>
        '''
    table_rows_street = ''
    for (day, city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in res:
        table_rows_street += table_rows.format(
            dt=day,
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
    # logging.info(table_rows_street)

    # Otrike
    hsql = '''
        SELECT 
            a.dt,
            a.city_id,
            b.name,
            'Otrike',
            a.broadcast_distance,                   --平均播单距离
            a.pickup_distance_done,                 --平均接驾距离完单
            a.pickup_distance_take,                 --平均接驾距离应答
            a.dispatch_obey_rate*100,                   --调度服从率
            '-',                            --应答率
            a.take_order_dur_done,                  --平均应答时长完单
            a.take_order_dur_take,                  --平均应答时长应答
            '-',                                    --乘客应答后取消率
            '-',                                    --司机应答后取消率
            a.tph_macro,                            --TPH
            a.online_range,                         --人均在线时长
            a.billing_order_dur_rate*100                --计费时长占比
        FROM {table} AS a 
        JOIN (SELECT 
                id,
                name 
            FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df 
            WHERE dt = '{pt}' AND 
                id < 999000
            ) AS b 
        ON a.city_id = b.id 
        WHERE a.dt = '{pt}' AND   
            a.city_id > 0 AND 
            a.product_id = 3 
        ORDER BY a.city_id ASC, a.dt DESC 
    '''.format(table=hive_table, pt=dt)
    logging.info(hsql)
    cursor.execute(hsql)
    res = cursor.fetchall()
    header_otrike = '''
            <tr style="background-color:#bf9001"><th colspan="15">宏观指标 - 分业务线Otrike</th></tr>
        '''
    table_rows_otrike = ''
    for (day, city_id, city, product, broadcast_distance, pickup_distance_done, pickup_distance_take,
         dispatch_obey_rate, take_rate, take_order_dur_done, take_order_dur_take, passanger_after_cancel_rate,
         driver_after_cancel_rate, tph_macro, online_range, billing_order_dur_rate) in res:
        table_rows_otrike += table_rows.format(
            dt=day,
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
    # logging.info(table_rows_otrike)

    cursor.close()

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
    # logging.info(mail_html_content)

    # send mail
    email_to = Variable.get("oride_app_oride_order_dispatch_d_receivers").split()
    result = is_alert(dt, [hive_table])
    if result:
        email_to = ['bigdata@opay-inc.com']
    # email_to = ['duo.wu@opay-inc.com']
    email_subject = 'oride调度算法指标_{}'.format(dt)
    send_email(
        email_to,
        email_subject,
        mail_html_content,
        mime_charset='utf-8'
    )


send_report = PythonOperator(
    task_id='send_report',
    python_callable=sendDispatchMail,
    provide_context=True,
    dag=dag
)

dependence_app_oride_order_pushed_d >> sleep_time
dependence_app_oride_driver_base_d >> sleep_time
dependence_app_oride_order_base_d >> sleep_time

sleep_time >> create_result_table_task
create_result_table_task >> insert_result_to_hive >> validate_partition_data >> send_report
