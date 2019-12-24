# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import csv
import logging
import codecs
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 4),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_street_association',
    schedule_interval="30 02 * * *",
    default_args=args)

table_names = ['oride_dw_ods.ods_sqoop_mass_driver_group_df',
               'oride_dw_ods.ods_sqoop_mass_rider_signups_df',
               'oride_dw_ods.ods_sqoop_base_data_driver_extend_df',
               'oride_dw_ods.ods_sqoop_base_data_driver_comment_df',
               'oride_dw.dwd_oride_order_base_include_test_di'
               ]

'''
校验分区代码
'''

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names": table_names,
        # 任务名称
        "task_name": "快车司机协会数据"
    },
    dag=dag
)

driver_group_validate_task = HivePartitionSensor(
    task_id="driver_group_validate_task",
    table="ods_sqoop_mass_driver_group_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

rider_signups_validate_task = HivePartitionSensor(
    task_id="rider_signups_validate_task",
    table="ods_sqoop_mass_rider_signups_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_extend_validate_task = HivePartitionSensor(
    task_id="data_driver_extend_validate_task",
    table="ods_sqoop_base_data_driver_extend_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_comment_validate_task = HivePartitionSensor(
    task_id="data_driver_comment_validate_task",
    table="ods_sqoop_base_data_driver_comment_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

oride_driver_timerange_validate_task = HivePartitionSensor(
    task_id="oride_driver_timerange_validate_task",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


insert_oride_street_association_di = HiveOperator(
    task_id='insert_oride_street_association_di',
    hql="""
        -- 协会数据
        with association_data as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_mass_driver_group_df
            WHERE
                dt='{{ ds }}'
        ),
        -- 司机数据
        driver_data as (
            SELECT
                t.driver_id,
                MAX(t.create_time) as create_time,
                MAX(struct(t.create_time, t.association_id)).col2 AS association_id,
                MAX(struct(t.create_time, t.license_number)).col2 AS license_number,
                MAX(struct(t.create_time, t.know_orider_extend)).col2 AS know_orider_extend,
                MAX(struct(t.create_time, t.status)).col2 AS status,
                MAX(struct(t.create_time, t.veri_time)).col2 AS veri_time,
                MAX(struct(t.create_time, t.online_test)).col2 AS online_test,
                MAX(struct(t.create_time, t.online_test_time)).col2 AS online_test_time,
                MAX(struct(t.create_time, t.record_by)).col2 AS record_by
            FROM
                (
                    SELECT * FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df WHERE dt='{{ ds }}' and association_id>0 and driver_id>0
                ) t
                INNER JOIN
                (
                    SELECT * FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df  WHERE dt='{{ ds }}' and serv_type=2
                ) t1 ON t1.id=t.driver_id
                INNER JOIN association_data t2 ON t2.id=t.association_id
            GROUP BY driver_id
        ),
        base_data as (
            SELECT
                association_id,
                count(distinct driver_id) as total_registered_drivers,
                count(distinct if(length(license_number)=0, null, driver_id)) as have_license_drivers,
                count(distinct if(length(know_orider_extend)>0 and record_by<>'' and from_unixtime(create_time, 'yyyy-MM-dd') ='{{ ds }}', driver_id, null)) as visit_drivers,
                count(distinct if(status=2 and from_unixtime(veri_time, 'yyyy-MM-dd')='{{ ds }}', driver_id, null)) as approved_drivers,
                count(distinct if(online_test=1 and from_unixtime(online_test_time, 'yyyy-MM-dd')='{{ ds }}', driver_id, null)) as test_drivers,
                count(distinct if(from_unixtime(create_time, 'yyyy-MM-dd')='{{ ds }}', driver_id, null)) as registered_drivers
            FROM
                driver_data
            GROUP BY association_id
        ),
        order_detail as (
            SELECT
                t.*,
                t1.association_id as association_id,
                nvl(t.price, 0) as payment_price,
                nvl(t.pay_amount, 0) as payment_amount
            FROM
                (
                    SELECT * FROM oride_dw.dwd_oride_order_base_include_test_di WHERE dt BETWEEN '{{ macros.ds_add(ds, -2) }}' AND '{{ ds }}'
                ) t
                INNER JOIN driver_data t1 ON t1.driver_id=t.driver_id
        ),
        order_data_today as (
            SELECT
                association_id,
                count(if(status=4 or status=5, id, null)) as completed_num, -- 完单量
                SUM(if(status=4 or status=5, payment_price, 0)) as total_price, -- 总计应付
                SUM(if(status=4 or status=5, payment_amount, 0)) as total_amount, -- 总计实付
                COUNT(distinct if((status=4 or status=5) and from_unixtime(create_time, 'yyyy-MM-dd')='{{ ds }}', driver_id, null)) as registered_completed_drivers, -- 注册并完单司机数
                COUNT(distinct driver_id) as take_drivers, -- 今日接单司机数
                count(if(status=6 and cancel_role=2, driver_id, null)) as canceled_drivers, -- 今日取消订单司机数
                count(if(status=4 or status=5, driver_id, null)) as completed_drivers -- 今日完单司机数
            FROM
                order_detail
            WHERE
                dt='{{ ds }}'
            GROUP BY association_id
        ),
        online_drivers_data as (
            SELECT
                t1.association_id,
                count(t.driver_id) as online_drivers -- 在线司机数
            FROM
                oride_dw_ods.ods_log_oride_driver_timerange t
                INNER JOIN driver_data t1 ON t1.driver_id=t.driver_id
            WHERE
                t.dt='{{ ds }}'
            GROUP BY
                t1.association_id
        ),
        offline_data as (
            SELECT
                t1.association_id,
                count(if(datediff('{{ ds }}', nvl(t2.l_dt, '1970-01-01'))=1, t1.driver_id, null)) as offline_drivers_2, -- 连续2日未上线司机数
                count(if(datediff('{{ ds }}', nvl(t2.l_dt, '1970-01-01'))>1, t1.driver_id, null)) as offline_drivers_3 -- 连续3日未上线司机数
            FROM
                driver_data t1
                LEFT JOIN
                (
                    SELECT
                        driver_id,
                        MAX(dt) as l_dt
                    FROM
                        oride_dw_ods.ods_log_oride_driver_timerange
                    WHERE
                        dt between '{{ macros.ds_add(ds, -2) }}' and '{{ ds }}'
                    GROUP BY
                        driver_id
                ) t2 ON t2.driver_id=t1.driver_id
            GROUP BY t1.association_id
        ),
        no_take_data as (
            SELECT
                t1.association_id,
                count(if(datediff('{{ ds }}', nvl(t2.l_dt, '1970-01-01'))=1, t1.driver_id, null)) as no_take_drivers_2, -- 连续2日未上线司机数
                count(if(datediff('{{ ds }}', nvl(t2.l_dt, '1970-01-01'))>1, t1.driver_id, null)) as no_take_drivers_3 -- 连续3日未上线司机数
            FROM
                driver_data t1
                LEFT JOIN
                (
                    SELECT
                        driver_id,
                        MAX(dt) as l_dt
                    FROM
                        order_detail
                    WHERE
                        driver_id>0
                    GROUP BY
                        driver_id
                ) t2 ON t2.driver_id=t1.driver_id
            GROUP BY t1.association_id
        ),
        no_completed_data as (
           SELECT
                t1.association_id,
                count(if(t2.driver_id is null, 1, null)) as no_completed_drivers_2_5 -- 连续2天未完成5单司机数
            FROM
                driver_data t1
                LEFT JOIN
                (
                    SELECT
                        driver_id
                    FROM
                        (
                        SELECT
                            driver_id,
                            count(if(dt='{{ ds }}', 1, null)) as counts_1,
                            count(if(dt='{{ macros.ds_add(ds, -1) }}', 1, null)) as counts_2
                        FROM
                            order_detail
                        WHERE
                            status in(4,5)
                        GROUP BY
                            driver_id
                        ) t
                    WHERE
                        counts_1>=5 and counts_2>=5
                ) t2 ON t2.driver_id=t1.driver_id
            GROUP BY t1.association_id
        ),
        comment_data as (
            SELECT
                t1.association_id,
                count(*) as bad_score_drivers -- 今日得差评司机数
            FROM
                driver_data t1
                LEFT JOIN
                (
                    SELECT
                        distinct driver_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_driver_comment_df
                    WHERE
                        from_unixtime(create_time, 'yyyy-MM-dd')='{{ ds }}' and score<=3


                ) t2 ON t2.driver_id=t1.driver_id
            GROUP BY t1.association_id
        )
        INSERT OVERWRITE TABLE oride_street_association_di PARTITION (dt='{{ ds }}')
        SELECT
            bd.association_id,
            nvl(bd.total_registered_drivers, 0), -- 总注册司机数
            nvl(bd.have_license_drivers, 0), -- 有驾照司机数
            nvl(bd.visit_drivers, 0), -- 到访数
            nvl(bd.approved_drivers, 0), -- 通过审核司机数
            nvl(bd.test_drivers, 0), -- 通过培训司机数
            nvl(bd.registered_drivers, 0),  -- 注册司机数
            nvl(odt.completed_num, 0), -- 完单量
            nvl(odt.total_price, 0), -- 总计应付
            nvl(odt.total_amount, 0), -- 总计实付
            nvl(odt.registered_completed_drivers, 0), -- 注册并完单司机数
            nvl(odt.take_drivers, 0), -- 今日接单司机数
            nvl(odt.completed_drivers, 0), -- 今日完单司机数
            nvl(odt.canceled_drivers, 0), -- 今日取消订单司机数
            nvl(ol.online_drivers, 0), -- 在线司机数
            nvl(od.offline_drivers_2, 0), -- 连续2日未上线司机数
            nvl(od.offline_drivers_3, 0), -- 连续3日未上线司机数
            nvl(otd.no_take_drivers_2, 0), -- 连续2日未接单司机数
            nvl(otd.no_take_drivers_3, 0), -- 连续3日未接单司机数
            nvl(ncd.no_completed_drivers_2_5, 0), -- 连续2天未完成5单司机数
            nvl(cd.bad_score_drivers, 0) --今日得差评司机数
        FROM
            base_data bd
            LEFT JOIN order_data_today odt ON odt.association_id=bd.association_id
            LEFT JOIN online_drivers_data ol on ol.association_id=bd.association_id
            LEFT JOIN offline_data od on od.association_id=bd.association_id
            LEFT JOIN no_take_data otd on otd.association_id=bd.association_id
            LEFT JOIN no_completed_data ncd on ncd.association_id=bd.association_id
            LEFT JOIN comment_data cd on cd.association_id=bd.association_id
    """,
    schema='oride_bi',
    dag=dag)


def send_oride_association_email(ds, **kwargs):
    cursor = get_hive_cursor()
    query = '''
        -- 今日数据
        with t_d as (
             SELECT
              *
            FROM
              oride_bi.oride_street_association_di
            WHERE
              dt='{dt}'
        ),
        -- 昨日数据
        l_d as (
            SELECT
              *
            FROM
              oride_bi.oride_street_association_di
            WHERE
              dt='{ld}'
        ),
        -- 上周数据
        l_w as (
            SELECT
              *
            FROM
              oride_bi.oride_street_association_di
            WHERE
              dt='{lw}'

        ),
        -- 协会数据
        association_data as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_mass_driver_group_df
            WHERE
                dt='{dt}'
        ),
        -- 城市数据
        city_data as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_base_data_city_conf_df
                
            WHERE
              dt='{dt}'
        )
        SELECT
            ad.name, -- 协会名称
            cd.name, -- 城市
            t_d.total_registered_drivers, -- 总注册司机数
            t_d.have_license_drivers, -- 有驾照司机数
            t_d.completed_num, -- 今日协会完单量
            round((t_d.completed_num - l_d.completed_num)/l_d.completed_num, 2),
            round((t_d.completed_num - l_w.completed_num)/l_w.completed_num, 2),
            round((t_d.total_price/t_d.completed_num), 2), -- 今日协会单均应付
            round(((t_d.total_price/t_d.completed_num) - (l_d.total_price/l_d.completed_num))/(l_d.total_price/l_d.completed_num),2),
            round(((t_d.total_price/t_d.completed_num) - (l_w.total_price/l_w.completed_num))/(l_w.total_price/l_w.completed_num),2),
            round((t_d.total_amount/t_d.completed_num), 2), -- 今日协会单均实付
            round(((t_d.total_amount/t_d.completed_num) - (l_d.total_amount/l_d.completed_num))/(l_d.total_amount/l_d.completed_num),2),
            round(((t_d.total_amount/t_d.completed_num) - (l_w.total_amount/l_w.completed_num))/(l_w.total_amount/l_w.completed_num),2),
            t_d.total_price, -- 今日协会GMV
            round((t_d.total_price - l_d.total_price)/l_d.total_price, 2),
            round((t_d.total_price - l_w.total_price)/l_w.total_price, 2),
            t_d.visit_drivers,  -- 今日到访司机数
            round((t_d.visit_drivers - l_d.visit_drivers)/l_d.visit_drivers, 2),
            round((t_d.visit_drivers - l_w.visit_drivers)/l_w.visit_drivers, 2),
            t_d.approved_drivers, -- 今日通过审核司机数
            round((t_d.approved_drivers - l_d.approved_drivers)/l_d.approved_drivers, 2),
            round((t_d.approved_drivers - l_w.approved_drivers)/l_w.approved_drivers, 2),
            t_d.test_drivers, -- 通过培训司机数
            round((t_d.test_drivers - l_d.test_drivers)/l_d.test_drivers, 2),
            round((t_d.test_drivers - l_w.test_drivers)/l_w.test_drivers, 2),
            t_d.registered_drivers,  -- 今日注册司机数
            round((t_d.registered_drivers - l_d.registered_drivers)/l_d.registered_drivers, 2),
            round((t_d.registered_drivers - l_w.registered_drivers)/l_w.registered_drivers, 2),
            t_d.registered_completed_drivers, -- 注册并完单司机数
            round((t_d.registered_completed_drivers - l_d.registered_completed_drivers)/l_d.registered_completed_drivers, 2),
            round((t_d.registered_completed_drivers - l_w.registered_completed_drivers)/l_w.registered_completed_drivers, 2),
            t_d.online_drivers, -- 在线司机数
            round((t_d.online_drivers - l_d.online_drivers)/l_d.online_drivers, 2),
            round((t_d.online_drivers - l_w.online_drivers)/l_w.online_drivers, 2),
            t_d.offline_drivers_2, -- 连续2日未上线司机数
            t_d.offline_drivers_3, -- 连续3日未上线司机数
            t_d.take_drivers, -- 今日接单司机数
            round((t_d.take_drivers - l_d.take_drivers)/l_d.take_drivers, 2),
            round((t_d.take_drivers - l_w.take_drivers)/l_w.take_drivers, 2),
            t_d.no_take_drivers_2, -- 连续2日未接单司机数
            t_d.no_take_drivers_3, -- 连续3日未接单司机数
            t_d.completed_drivers, -- 今日完单司机数
            round((t_d.completed_drivers - l_d.completed_drivers)/l_d.completed_drivers, 2),
            round((t_d.completed_drivers - l_w.completed_drivers)/l_w.completed_drivers, 2),
            t_d.no_completed_drivers_2_5, -- 连续2天未完成5单司机数
            t_d.canceled_drivers, -- 今日取消订单司机数
            round((t_d.canceled_drivers - l_d.canceled_drivers)/l_d.canceled_drivers, 2),
            round((t_d.canceled_drivers - l_w.canceled_drivers)/l_w.canceled_drivers, 2),
            t_d.bad_score_drivers, --今日得差评司机数
            round((t_d.bad_score_drivers - l_d.bad_score_drivers)/l_d.bad_score_drivers, 2),
            round((t_d.bad_score_drivers - l_w.bad_score_drivers)/l_w.bad_score_drivers, 2)
        FROM
            t_d
            INNER JOIN association_data ad on ad.id=t_d.association_id
            INNER JOIN city_data cd ON cd.id=ad.city
            LEFT JOIN l_d ON l_d.association_id=t_d.association_id
            LEFT JOIN l_w ON l_w.association_id=t_d.association_id
    '''.format(dt=ds, ld=airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))
    logging.info('Executing: %s', query)
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [
        'group_name',
        'city',
        'total_register_drivers',
        'have_license_drivers',
        'today_finish_orders',
        'compare_yesterday',
        'compare_last_week',
        'today_average_order_should_pay',
        'compare_yesterday',
        'compare_last_week',
        'today_average_order_actually_pay',
        'compare_yesterday',
        'compare_last_week',
        'today_GMV',
        'compare_yesterday',
        'compare_last_week',
        'today_checkin_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_approved_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_tested_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_register_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_register&finish_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_online_drivers',
        'compare_yesterday',
        'compare_last_week',
        'two_days_notonline_drivers',
        'three_days_notonline_drivers',
        'today_accept_order_drivers',
        'compare_yesterday',
        'compare_last_week',
        'two_days_notaccept_order_drivers',
        'three_days_notaccept_order_drivers',
        'today_finish_order_drivers',
        'compare_yesterday',
        'compare_last_week',
        'two_days_notfinish_5orders_drivers',
        'today_cancel_order_drivers',
        'compare_yesterday',
        'compare_last_week',
        'today_rate_score≤3_drivers',
        'compare_yesterday',
        'compare_last_week',
    ]
    file_name = '/tmp/oride_street_association_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)
    # send mail

    email_to = Variable.get("oride_street_association_receivers").split()
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']

    email_subject = 'oride快车司机协会数据{dt}'.format(dt=ds)
    email_body = ''
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


oride_association_email = PythonOperator(
    task_id='oride_association_email',
    python_callable=send_oride_association_email,
    provide_context=True,
    dag=dag
)

validate_partition_data >> data_driver_extend_validate_task >> insert_oride_street_association_di
validate_partition_data >> data_driver_comment_validate_task >> insert_oride_street_association_di
validate_partition_data >> driver_group_validate_task >> insert_oride_street_association_di
validate_partition_data >> rider_signups_validate_task >> insert_oride_street_association_di
validate_partition_data >> oride_driver_timerange_validate_task >> insert_oride_street_association_di
insert_oride_street_association_di >> oride_association_email
