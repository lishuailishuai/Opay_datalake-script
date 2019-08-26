# -*- coding: utf-8 -*-
"""
ofood商户排行数据
"""
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.impala_plugin import ImpalaOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn, get_db_conf
from utils.validate_metrics_utils import *
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from plugins.comwx import ComwxApi
import time
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_ofood_merchant_offline_tag_d',
    schedule_interval="00 04 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

"""
依赖采集完成
"""
dependence_ods_sqoop_base_jh_order_df = UFileSensor(
    task_id='dependence_ods_sqoop_base_jh_order_df',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="ofood_dw_sqoop/food_operapay_co/jh_order",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)


dependence_ods_sqoop_base_jh_order_log_df = UFileSensor(
    task_id='dependence_ods_sqoop_base_jh_order_log_df',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="ofood_dw_sqoop/food_operapay_co/jh_order_log",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)


dependence_ods_sqoop_base_jh_waimai_comment_df = UFileSensor(
    task_id='dependence_ods_sqoop_base_jh_waimai_comment_df',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="ofood_dw_sqoop/food_operapay_co/jh_waimai_comment",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_sqoop_base_jh_order_time_df = UFileSensor(
    task_id='dependence_ods_sqoop_base_jh_order_time_df',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="ofood_dw_sqoop/food_operapay_co/jh_order_time",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

"""
end
"""

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)




def get_data_from_impala(**op_kwargs):
    ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    sql = '''
        WITH
        --商家取消单
        merchant_data as 
        (
            select 
                a1.shop_id, 
                count(distinct a1.order_id) cancel_order 
            from (select 
                    shop_id,
                    order_id 
                from ofood_dw.ods_sqoop_base_jh_order_df 
                where dt='{pt}' and 
                    order_status='-2' and 
                    from_unixtime(dateline,'yyyy-MM-dd') between date_sub('{pt}', 6) and '{pt}'
                ) as a1
            join (select 
                    order_id
                from ofood_dw.ods_sqoop_base_jh_order_log_df 
                where dt='{pt}' and 
                    log like '%Merchant cancelling order%'
                ) as a2
            on a1.order_id=a2.order_id
            group by a1.shop_id
        ),
        --评价单小于等于2
        score_data as (
            select 
                a1.shop_id, 
                count(distinct a1.order_id) as scorelttwo
            from (select 
                    shop_id,
                    order_id
                from ofood_dw.ods_sqoop_base_jh_order_df 
                where dt='{pt}' and 
                    order_status='8' and 
                    from_unixtime(dateline,'yyyy-MM-dd') between date_sub('{pt}', 6) and '{pt}'
                ) as a1
            join (select 
                    order_id
                from ofood_dw.ods_sqoop_base_jh_waimai_comment_df 
                where dt='{pt}' and 
                    score_peisong<=2
                ) as a2
            on a1.order_id = a2.order_id
            group by a1.shop_id
        ),
        --大于90分的单子
        order_data as (
            select 
                s.shop_id, 
                count(distinct s.order_id) as ordersgt90
            from (select 
                    a.shop_id, 
                    a.order_id,
                    datediff(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'),from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss'))*24*60+
                    (hour(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'))-hour(from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss')))*60+(minute(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'))-minute(from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss'))) as tm
                from (select 
                        shop_id,
                        order_id,
                        pay_time
                    from ofood_dw.ods_sqoop_base_jh_order_df 
                    where dt='{pt}' and 
                        order_status = '8' and 
                        from_unixtime(dateline,'yyyy-MM-dd')  between date_sub('{pt}', 6) and '{pt}'
                    ) as a
                join (select 
                        order_id,
                        order_compltet_time
                    from ofood_dw.ods_sqoop_base_jh_order_time_df 
                    where dt='{pt}'
                    ) as b
                on a.order_id=b.order_id
                group by a.shop_id, a.order_id, datediff(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'),from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss'))*24*60+
                    (hour(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'))-hour(from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss')))*60+(minute(from_unixtime(b.order_compltet_time,'yyyy-MM-dd HH:mm:ss'))-minute(from_unixtime(a.pay_time,'yyyy-MM-dd HH:mm:ss')))
                ) s 
            where s.tm>90
            group by s.shop_id
        ),
        --近7天下单量
        order_7day as (
            select 
                shop_id, 
                count(distinct order_id) as orders 
            from ofood_dw.ods_sqoop_base_jh_order_df 
            where dt='{pt}' and 
                from_unixtime(dateline,'yyyy-MM-dd')  between date_sub('{pt}', 6) and '{pt}'
            group by shop_id
        ),
        --近30天完单量
        order_30day as (
            select 
                shop_id, 
                count(distinct order_id) as orderfinish
            from ofood_dw.ods_sqoop_base_jh_order_df 
            where dt='{pt}' and 
                from_unixtime(dateline,'yyyy-MM-dd')  between date_sub('{pt}', 29) and '{pt}' and 
                order_status = '8'
            group by shop_id
        )

        --结果集
        select 
            order_7day.shop_id,
            nvl(merchant_data.cancel_order, 0),
            nvl(score_data.scorelttwo, 0),
            nvl(order_data.ordersgt90, 0),
            order_7day.orders,
            nvl(order_30day.orderfinish, 0),
            unix_timestamp('{pt}', 'yyyy-MM-dd')
        from order_7day
        left join merchant_data on order_7day.shop_id = merchant_data.shop_id
        left join score_data on order_7day.shop_id = score_data.shop_id 
        left join order_data on order_7day.shop_id = order_data.shop_id 
        left join order_30day on order_7day.shop_id = order_30day.shop_id 
    '''.format(
        pt=ds
    )
    logging.info(sql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(sql)
    result = hive_cursor.fetchall()

    mysql_conn = get_db_conn('mysql_bi')
    mcursor = mysql_conn.cursor()
    __data_to_mysql(mcursor, result,
                    ['shop_id', 'last7_cancel_order', 'last7_low_scores_2_order', 'last7_forward_time_90_order',
                     'last7_request_order', 'last30_finish_order', 'update_time'],
                    '''
                        last7_cancel_order=values(last7_cancel_order),
                        last7_low_scores_2_order=values(last7_low_scores_2_order),
                        last7_forward_time_90_order=values(last7_forward_time_90_order),
                        last7_request_order=values(last7_request_order),
                        last30_finish_order=values(last30_finish_order),
                        update_time=values(update_time)
                    '''
                    )


def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into bi.ofood_merchant_offline_tag ({})'.format(','.join(column))
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (shop_id, cancel_order, scorelttwo, ordersgt90, orders, orderfinish, upt) in data:

            row = [shop_id, cancel_order, scorelttwo, ordersgt90, orders, orderfinish, upt]
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                logging.info(esql.format(isql, sval, update))
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            logging.info(esql.format(isql, sval, update))
            conn.execute(esql.format(isql, sval, update))
    except BaseException as e:
        logging.info(e)
        return


get_data_from_impala_task = PythonOperator(
    task_id='get_data_from_impala_task',
    python_callable=get_data_from_impala,
    provide_context=True,
    dag=dag
)


comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')


def check_ds_data(**op_kwargs):
    ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    sql = '''
        select count(1) as cnt from bi.ofood_merchant_offline_tag where from_unixtime(update_time,'%Y-%m-%d')='{pt}'
    '''.format(pt=ds)
    mysql_conn = get_db_conn('mysql_bi')
    mcursor = mysql_conn.cursor()
    mcursor.execute(sql)
    res = mcursor.fetchall()
    logging.info(sql)
    logging.info(res)
    logging.info(isinstance(res, tuple))
    logging.info(len(res))
    logging.info(res[0])
    if res is None or not isinstance(res, tuple) or len(res) <= 0:
        comwx.postAppMessage('ofood商家订单指标缺少{}数据, 请及时排查'.format(ds), '271')
    else:
        (cnt,) = res[0]
        logging.info(cnt)
        if cnt <= 0:
            comwx.postAppMessage('ofood商家订单指标缺少{}数据, 请及时排查'.format(ds), '271')


check_data = PythonOperator(
    task_id='check_data',
    python_callable=check_ds_data,
    provide_context=True,
    dag=dag
)


dependence_ods_sqoop_base_jh_order_df >> sleep_time
dependence_ods_sqoop_base_jh_order_log_df >> sleep_time
dependence_ods_sqoop_base_jh_waimai_comment_df >> sleep_time
dependence_ods_sqoop_base_jh_order_time_df >> sleep_time

sleep_time >> get_data_from_impala_task >> check_data
