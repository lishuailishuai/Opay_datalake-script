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

create_oride_street_association_di = HiveOperator(
    task_id='create_oride_street_association_di',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_street_association_di (
          association_id int,  -- 协会id
          total_registered_drivers int, -- 总注册司机数
          have_license_drivers int, -- 有驾照司机数
          visit_drivers int, -- 到访数
          approved_drivers int, -- 通过审核司机数
          test_drivers int, -- 通过培训司机数
          registered_drivers int,  -- 注册司机数
          completed_num int, -- 完单量
          total_price decimal(10,2), -- 总计应付
          total_amount decimal(10,2), -- 总计实付
          registered_completed_drivers int, -- 注册并完单司机数
          take_drivers int, -- 今日接单司机数
          completed_drivers int, -- 今日完单司机数
          canceled_drivers int, -- 今日取消订单司机数
          online_drivers int, -- 在线司机数
          offline_drivers_2 int, -- 连续2日未上线司机数
          offline_drivers_3 int, -- 连续3日未上线司机数
          no_take_drivers_2 int, -- 连续2日未接单司机数
          no_take_drivers_3 int, -- 连续3日未接单司机数
          no_completed_drivers_2_5 int, -- 连续2天未完成5单司机数
          bad_score_drivers int --今日得差评司机数
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='oride_bi',
    dag=dag)

insert_oride_street_association_di  = HiveOperator (
    task_id='insert_oride_street_association_di',
    hql="""
        -- 协会数据
        with association_data as (
            SELECT
                *
            FROM
                oride_dw.ods_sqoop_mass_driver_group_df
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
                    SELECT * FROM oride_dw.ods_sqoop_mass_rider_signups_df WHERE dt='{{ ds }}' and association_id>0 and driver_id>0
                ) t
                INNER JOIN
                (
                    SELECT * FROM oride_db.data_driver_extend  WHERE dt='{{ ds }}' and serv_type=2
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
                nvl(t2.price, 0) as payment_price,
                nvl(t2.amount, 0) as payment_amount
            FROM
                (
                    SELECT * FROM oride_db.data_order WHERE dt='{{ ds }}' AND from_unixtime(create_time, 'yyyy-MM-dd') between '{{ macros.ds_add(ds, -2) }}' AND '{{ ds }}'
                ) t
                INNER JOIN driver_data t1 ON t1.driver_id=t.driver_id
                LEFT JOIN
                (
                    SELECT * FROM oride_db.data_order_payment WHERE dt='{{ ds }}' AND from_unixtime(create_time, 'yyyy-MM-dd') between '{{ macros.ds_add(ds, -2) }}' AND '{{ ds }}'
                ) t2 ON t2.id=t.id
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
                from_unixtime(create_time, 'yyyy-MM-dd')='{{ ds }}'
            GROUP BY association_id
        ),
        online_drivers_data as (
            SELECT
                t1.association_id,
                count(t.driver_id) as online_drivers -- 在线司机数
            FROM
                oride_bi.oride_driver_timerange t
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
                        oride_bi.oride_driver_timerange
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
                        MAX(from_unixtime(create_time, 'yyyy-MM-dd')) as l_dt
                    FROM
                        order_detail
                    WHERE
                        from_unixtime(create_time, 'yyyy-MM-dd') between '{{ macros.ds_add(ds, -2) }}' AND '{{ ds }}' and driver_id>0
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
                            count(if(from_unixtime(create_time, 'yyyy-MM-dd')='{{ ds }}', 1, null)) as counts_1,
                            count(if(from_unixtime(create_time, 'yyyy-MM-dd')='{{ macros.ds_add(ds, -1) }}', 1, null)) as counts_2
                        FROM
                            order_detail
                        WHERE
                            from_unixtime(create_time, 'yyyy-MM-dd') between '{{ macros.ds_add(ds, -1) }}' AND '{{ ds }}' and status in(4,5)
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
                        oride_db.data_driver_comment
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
                oride_dw.ods_sqoop_mass_driver_group_df
            WHERE
                dt='{dt}'
        ),
        -- 城市数据
        city_data as (
            SELECT
                *
            FROM
                oride_db.data_city_conf
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
    '''.format(dt=ds, ld= airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))
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
    email_subject = 'oride快车司机协会数据{dt}'.format(dt=ds)
    email_body='快车司机协会数据，请查收。 附件中文乱码解决:使用记事本打开CSV文件，“文件”->“另存为”，编码方式选择ANSI，保存完毕后，用EXCEL打开，即可'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')

oride_association_email = PythonOperator(
    task_id='oride_association_email',
    python_callable=send_oride_association_email,
    provide_context=True,
    dag=dag
)

