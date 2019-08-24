import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.email import send_email
import logging
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 6, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
}

dag = airflow.DAG(
    'oride_driver_transport_capacity_daily',
    schedule_interval="45 02 * * *",
    default_args=args)



table_names = ['oride_db.data_order',
               'oride_db.data_driver_extend',
               'oride_db.data_city_conf',
               'oride_db.data_order_payment',
               'oride_bi.server_magic_push_detail',
               'oride_bi.oride_driver_timerange'
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
        "task_name": "司机运力日报-快车"
    },
    dag=dag
)

data_driver_extend_validate_task = HivePartitionSensor(
    task_id="data_driver_extend_validate_task",
    table="data_driver_extend",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_order_validate_task = HivePartitionSensor(
    task_id="data_order_validate_task",
    table="data_order",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_city_conf_validate_task = HivePartitionSensor(
    task_id="data_city_conf_validate_task",
    table="data_city_conf",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_order_payment_validate_task = HivePartitionSensor(
    task_id="data_order_payment_validate_task",
    table="data_order_payment",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

server_magic_push_detail_validate_task = HivePartitionSensor(
    task_id="server_magic_push_detail_validate_task",
    table="server_magic_push_detail",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

oride_driver_timerange_validate_task = HivePartitionSensor(
    task_id="oride_driver_timerange_validate_task",
    table="oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

insert_driver_metrics = HiveOperator(
    task_id='insert_driver_metrics',
    hql='''
        set hive.execution.engine=mr;
        set mapreduce.map.java.opts=-Xmx1800m -XX:-UseGCOverheadLimit;
        set mapreduce.reduce.java.opts=-Xmx2048m;
        set mapreduce.map.memory.mb=2048;
        set mapreduce.reduce.memory.mb=3072;

        -- 快车司机维度表
        with driver_dim as (
            select 
            c.name city_name,
            e.id ,
            e.serv_type,
            e.register_time
            from 
            (
                select 
                id,
                serv_type,
                city_id,
                register_time
                from 
                oride_db.data_driver_extend
                where dt = '{{ ds }}'
            ) e
            join 
            (
                select 
                * 
                from oride_db.data_city_conf
                where dt = '{{ ds }}' and id not in (999001,999002)
            ) c on  e.city_id = c.id 
        ),
        
        
        online_data as (
            select 
            d.city_name city_name,
            d.serv_type serv_type,
            count(d.id) online_driver_num -- 在线司机数
            from 
            driver_dim d 
            join 
            (
                select 
                driver_id,
                driver_onlinerange
                from 
                oride_bi.oride_driver_timerange
                where dt = '{{ ds }}'
            ) t on d.id = t.driver_id
            group by d.city_name,d.serv_type
        ),
        
        
        -- 订单基础表
        order_base as (
            SELECT
                *
            FROM
                oride_db.data_order
            WHERE
                dt='{{ ds }}'
                AND city_id != 999001
                AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
        ),
        
        -- 完单司机在线时长
        completed_driver_online as (
            SELECT
                d.city_name,
                d.serv_type,
                SUM(nvl(t1.do_range, 0) + nvl(t2.driver_freerange, 0)) AS driver_onlinerange_sum
            FROM
                (
                    -- 完单司机做单时长
                    SELECT
                        do.dt,
                        do.driver_id,
                        sum(
                            CASE do.status
                                WHEN 4 THEN abs(do.arrive_time-do.take_time)
                                WHEN 5 THEN abs(do.finish_time-do.take_time)
                                WHEN 6 THEN abs(do.cancel_time-do.take_time)
                                ELSE 0
                            END
                         )  as do_range
                    FROM
                        (
                            -- 完单司机
                            SELECT
                                distinct driver_id
                            FROM
                                order_base
                            WHERE
                                status in (4,5)
                        ) dd
                        INNER JOIN order_base do ON do.driver_id=dd.driver_id
                    GROUP BY do.dt,do.driver_id
                ) t1
                INNER JOIN
                (
                    -- 空闲时长
                    SELECT
                        *
                    FROM
                        oride_bi.oride_driver_timerange
                    WHERE
                        dt='{{ ds }}'
                ) t2 ON t2.driver_id = t1.driver_id
                join driver_dim d on d.id = t1.driver_id
            GROUP BY d.city_name,d.serv_type
        ),
        
        
        order_data as (
                select 
                d.city_name ,
                d.serv_type serv_type,
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' ,do.id,null)) accept_num, --当日接单量
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' ,do.driver_id,null)) accept_driver_num, --当日接单司机数
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.id,null)) onride_num, -- 当日完单量
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.driver_id,null)) onride_driver_num, -- 完单司机数
                count(distinct if(do.status = 4 or do.status = 5,do.driver_id,null)) agg_onride_driver_num, -- 累计完单司机数
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and from_unixtime(d.register_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.driver_id,null)) register_and_onride_driver_num, -- 当日注册且完单司机数
                sum(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}'
                and do.arrive_time > 0 and do.status in (4,5),do.arrive_time - do.pickup_time,0))  duration_sum, -- 计费时长
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and do.status = 6 and do.cancel_role = 2,do.id,null)) driver_cancel_num --司机取消订单数
                from
                (
                    select 
                    *
                    from order_base
                ) do
                join driver_dim d on do.driver_id = d.id
                group by d.city_name,d.serv_type
        ),
        
        
        order_pay as (
            select 
            d.city_name city_name,
            d.serv_type serv_type,
            count(1) order_pay_num, --支付订单数 
            sum(p.price) price_sum, --应付总金额
            sum(p.amount) amount_sum --实付总金额
            from 
            (   
                select 
                id,
                driver_id,
                price,
                amount
                from 
                oride_db.data_order_payment 
                where dt = '{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}'  and status = 1
            ) p 
            join driver_dim d on p.driver_id = d.id
            group by d.city_name,d.serv_type
        ),
        
        
        
        
        driver_register as (
            select 
            city_name,
            serv_type,
            count(if(from_unixtime(register_time,'yyyy-MM-dd') = '{{ ds }}',id,null)) register_driver_num, --注册司机数
            count(id) agg_register_driver_num  -- 累计注册司机数
            from 
            driver_dim 
            group by city_name,serv_type
        ),
        
        order_push as (
            select 
            t.city_name city_name,
            t.serv_type serv_type,
            sum(t.push_num) push_order_times_num, --推送订单次数 
            sum(t.order_num) push_order_num, -- 推送订单量
            count(t.driver_id)  push_driver_num -- 推送司机数
            from 
            (
                select 
                d.city_name city_name,
                d.serv_type serv_type,
                s.driver_id driver_id,
                count(s.order_id) push_num,
                count(distinct(s.order_id)) order_num
                from 
                (
                    select 
                    order_id,
                    driver_id
                    from oride_bi.server_magic_push_detail
                    where dt = '{{ ds }}' and success = 1
                ) s
                join driver_dim d on d.id = s.driver_id
                group by d.city_name,d.serv_type,s.driver_id
            ) t
            group by t.city_name,t.serv_type
        ),
        
        
        order_push_driver as (
            select 
            o.city_name,
            s.serv_type,
            count(s.driver_num)  push_driver_num, -- 推送司机数
            sum(s.driver_num) push_order_to_driver_num -- 订单推送司机数
            from 
            (
                select 
                t.order_id,
                t.serv_type,
                max(t.driver_num) driver_num
                from 
                (
                    select 
                    s.order_id,
                    d.serv_type,
                    s.round,
                    count(driver_id) driver_num
                    from 
                    (
                        select 
                        *
                        from 
                        oride_bi.server_magic_push_detail
                        where dt = '{{ ds }}' and success = 1 
                    ) s 
                    join driver_dim d on d.id = s.driver_id
                    group by s.order_id,d.serv_type,s.round
                ) t
                group by t.order_id,t.serv_type
            ) s 
            join 
            (
                select 
                c.name city_name, 
                o.id
                from oride_db.data_order o 
                join oride_db.data_city_conf c on c.dt = '{{ ds }}' and o.city_id = c.id
                where o.dt = '{{ ds }}' and o.serv_type = 2
            ) o on s.order_id = o.id 
            group by o.city_name,s.serv_type
        )
        
        
        insert overwrite table oride_bi.oride_all_driver_capacity_metrics_info partition (dt='{{ ds }}')
        select 
        od.city_name,
        od.serv_type,
        nvl(opd.push_order_to_driver_num,0),
        nvl(opu.push_driver_num,0),
        nvl(opu.push_order_times_num,0),
        nvl(opu.push_order_num,0),
        nvl(oda.online_driver_num,0),
        nvl(cdo.driver_onlinerange_sum,0),
        od.duration_sum,
        od.onride_num,
        od.accept_driver_num,
        od.onride_driver_num,
        od.accept_num,
        od.register_and_onride_driver_num,
        od.agg_onride_driver_num,
        od.driver_cancel_num,
        nvl(dr.register_driver_num,0),
        nvl(dr.agg_register_driver_num,0),
        nvl(op.order_pay_num,0),
        nvl(op.price_sum,0),
        nvl(op.amount_sum,0)
        
        from 
        order_data od 
        left join online_data oda on od.city_name = oda.city_name and od.serv_type = oda.serv_type
        left join order_pay op on od.city_name = op.city_name and od.serv_type = op.serv_type
        left join driver_register dr on od.city_name = dr.city_name and od.serv_type = dr.serv_type
        left join order_push opu on od.city_name = opu.city_name and od.serv_type = opu.serv_type
        left join order_push_driver opd on od.city_name = opd.city_name and od.serv_type = opd.serv_type
        left join completed_driver_online cdo on od.city_name = cdo.city_name and od.serv_type = cdo.serv_type
        ;


        ''',
    schema='oride_bi',
    dag=dag)


def send_fast_report_email(ds, **kwargs):
    cursor = get_hive_cursor()
    sql = '''
            select 
            'ALL' city_name,
            nvl(round(sum(push_order_times_num)/sum(push_driver_num),2),0) push_driver_times_avg,
            nvl(round(sum(push_order_num)/sum(push_driver_num),2),0) push_driver_order_avg,
            nvl(round(sum(driver_onlinerange_sum)/3600,2),0) driver_onlinerange_sum,
            nvl(round(sum(driver_onlinerange_sum)/(3600 * sum(onride_driver_num)),2),0) driver_onlinerange_rate,
            concat(cast(nvl(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2),0) as string),'%') duration_rate,
            nvl(round(sum(onride_num)/sum(onride_driver_num),2),0) onride_driver_order_avg,
            nvl(sum(online_driver_num),0) online_driver_num,
            nvl(sum(accept_driver_num),0) accept_driver_num,
            nvl(sum(onride_driver_num),0) onride_driver_num,
            nvl(sum(register_driver_num),0) register_driver_num,
            nvl(sum(register_and_onride_driver_num),0) register_and_onride_driver_num,
            nvl(sum(agg_register_driver_num),0) agg_register_driver_num,
            nvl(sum(agg_onride_driver_num),0) agg_onride_driver_num,
            nvl(round(sum(price_sum)/sum(order_pay_num),2),0) order_price_avg,
            nvl(round(sum(amount_sum)/sum(order_pay_num),2),0) order_amount_avg
            from 
            oride_bi.oride_all_driver_capacity_metrics_info
            where dt = '{dt}' and serv_type = 2


    '''.format(dt=ds)

    city_sql = '''
            select 
            city_name,
            nvl(round(sum(push_order_times_num)/sum(push_driver_num),2),0) push_driver_times_avg,
            nvl(round(sum(push_order_num)/sum(push_driver_num),2),0) push_driver_order_avg,
            nvl(round(sum(driver_onlinerange_sum)/3600,2),0) driver_onlinerange_sum,
            nvl(round(sum(driver_onlinerange_sum)/(3600 * sum(onride_driver_num)),2),0) driver_onlinerange_rate,
            concat(cast(nvl(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2),0) as string),'%') duration_rate,
            nvl(round(sum(onride_num)/sum(onride_driver_num),2),0) onride_driver_order_avg,
            nvl(sum(online_driver_num),0) online_driver_num,
            nvl(sum(accept_driver_num),0) accept_driver_num,
            nvl(sum(onride_driver_num),0) onride_driver_num,
            nvl(sum(register_driver_num),0) register_driver_num,
            nvl(sum(register_and_onride_driver_num),0) register_and_onride_driver_num,
            nvl(sum(agg_register_driver_num),0) agg_register_driver_num,
            nvl(sum(agg_onride_driver_num),0) agg_onride_driver_num,
            nvl(round(sum(price_sum)/sum(order_pay_num),2),0) order_price_avg,
            nvl(round(sum(amount_sum)/sum(order_pay_num),2),0) order_amount_avg
            from 
            oride_bi.oride_all_driver_capacity_metrics_info
            where dt = '{dt}' and serv_type = 2
            group by city_name
            order by city_name

    '''.format(dt=ds)

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
                    background-color: #CCE0F1;
                    //color: white;
                    width: 100px;
                }}
            </style>
            </head>
            <body>
                <table width="100%" class="table">
                    <caption>
                        <h3>快车指标数据</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="2" style="text-align: center;">总需求</th>
                            <th colspan="7" style="text-align: center;">总供给</th>
                            <th colspan="4" style="text-align: center;">招募</th>
                            <th colspan="2" style="text-align: center;">财务</th>
                        </tr>
                        <tr>
                            <th>城市</th>
                            <!--总需求-->
                            <th>人均推单次数</th>
                            <th>人均推送订单数</th>
                            <!--总供给-->
                            <th>总在线时长</th>
                            <th>人均在线时长</th>
                            <th>计费时长占比</th>
                            <th>人均完单数</th>
                            <th>在线司机数</th>
                            <th>接单司机数</th>
                            <th>完单司机数</th>
                            <!--招募-->
                            <th>注册司机数</th>
                            <th>注册且完单司机数</th>
                            <th>累计注册司机数</th>
                            <th>累计完单司机数</th>

                            <!--财务-->
                            <th>单均应付</th>
                            <th>单均实付</th>
                            <!--司机考核-->

                        </tr>
                    </thead>
                    {rows}
                </table>
            </body>
            </html>
            '''

    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()

    logging.info(city_sql)
    cursor.execute(city_sql)
    city_res = cursor.fetchall()

    row_html = ''
    tr_fmt = '''
                <tr>{row}</tr>
            '''

    row_fmt = '''
                <th>{city_name}</th>
                <th>{order_time_push_driver_avg}</th>
                <th>{order_push_driver_avg}</th>
                <th>{driver_online_time_sum}</th>
                <th>{driver_online_avg}</th>
                <th>{duration_rate}</th>
                <th>{onride_avg}</th>
                <th>{online_driver_num}</th>
                <th>{accpet_driver_num}</th>
                <th>{onride_driver_num}</th>
                <th>{register_driver_num}</th>
                <th>{register_and_onride_driver_num}</th>
                <th>{agg_register_driver_num}</th>
                <th>{agg_onride_driver_num}</th>
                <th>{price_avg}</th>
                <th>{amount_avg}</th>
        '''

    for data in res:
        [
            city_name,
            order_time_push_driver_avg,
            order_push_driver_avg,
            driver_online_time_sum,
            driver_online_avg,
            duration_rate,
            onride_avg,
            online_driver_num,
            accpet_driver_num,
            onride_driver_num,
            register_driver_num,
            register_and_onride_driver_num,
            agg_register_driver_num,
            agg_onride_driver_num,
            price_avg,
            amount_avg
        ] = data

        row = row_fmt.format(
            city_name=city_name,
            order_time_push_driver_avg=order_time_push_driver_avg,
            order_push_driver_avg=order_push_driver_avg,
            driver_online_time_sum=driver_online_time_sum,
            driver_online_avg=driver_online_avg,
            duration_rate=duration_rate,
            onride_avg=onride_avg,
            online_driver_num=online_driver_num,
            accpet_driver_num=accpet_driver_num,
            onride_driver_num=onride_driver_num,
            register_driver_num=register_driver_num,
            register_and_onride_driver_num=register_and_onride_driver_num,
            agg_register_driver_num=agg_register_driver_num,
            agg_onride_driver_num=agg_onride_driver_num,
            price_avg=price_avg,
            amount_avg=amount_avg
        )

        row_html += tr_fmt.format(row=row)

    for data in city_res:
        [
            city_name,
            order_time_push_driver_avg,
            order_push_driver_avg,
            driver_online_time_sum,
            driver_online_avg,
            duration_rate,
            onride_avg,
            online_driver_num,
            accpet_driver_num,
            onride_driver_num,
            register_driver_num,
            register_and_onride_driver_num,
            agg_register_driver_num,
            agg_onride_driver_num,
            price_avg,
            amount_avg
        ] = data

        row = row_fmt.format(
            city_name=city_name,
            order_time_push_driver_avg=order_time_push_driver_avg,
            order_push_driver_avg=order_push_driver_avg,
            driver_online_time_sum=driver_online_time_sum,
            driver_online_avg=driver_online_avg,
            duration_rate=duration_rate,
            onride_avg=onride_avg,
            online_driver_num=online_driver_num,
            accpet_driver_num=accpet_driver_num,
            onride_driver_num=onride_driver_num,
            register_driver_num=register_driver_num,
            register_and_onride_driver_num=register_and_onride_driver_num,
            agg_register_driver_num=agg_register_driver_num,
            agg_onride_driver_num=agg_onride_driver_num,
            price_avg=price_avg,
            amount_avg=amount_avg
        )

        row_html += tr_fmt.format(row=row)

    html = html_fmt.format(rows=row_html, dt=ds)

    # send mail

    email_to = Variable.get("oride_fast_driver_transport_metrics_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']
        # email_to = ['nan.li@opay-inc.com']

    email_subject = '司机运力日报-快车_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_fast_report = PythonOperator(
    task_id='send_fast_report',
    python_callable=send_fast_report_email,
    provide_context=True,
    dag=dag
)


def send_otrike_report_email(ds, **kwargs):
    cursor = get_hive_cursor()
    sql = '''
            select 
            'ALL' city_name,
            nvl(round(sum(push_order_times_num)/sum(push_driver_num),2),0) push_driver_times_avg,
            nvl(round(sum(push_order_num)/sum(push_driver_num),2),0) push_driver_order_avg,
            nvl(round(sum(driver_onlinerange_sum)/3600,2),0) driver_onlinerange_sum,
            nvl(round(sum(driver_onlinerange_sum)/(3600 * sum(onride_driver_num)),2),0) driver_onlinerange_rate,
            concat(cast(nvl(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2),0) as string),'%') duration_rate,
            nvl(round(sum(onride_num)/sum(onride_driver_num),2),0) onride_driver_order_avg,
            nvl(sum(online_driver_num),0) online_driver_num,
            nvl(sum(accept_driver_num),0) accept_driver_num,
            nvl(sum(onride_driver_num),0) onride_driver_num,
            nvl(sum(register_driver_num),0) register_driver_num,
            nvl(sum(register_and_onride_driver_num),0) register_and_onride_driver_num,
            nvl(sum(agg_register_driver_num),0) agg_register_driver_num,
            nvl(sum(agg_onride_driver_num),0) agg_onride_driver_num,
            nvl(round(sum(price_sum)/sum(order_pay_num),2),0) order_price_avg,
            nvl(round(sum(amount_sum)/sum(order_pay_num),2),0) order_amount_avg
            from 
            oride_bi.oride_all_driver_capacity_metrics_info
            where dt = '{dt}' and serv_type = 3


    '''.format(dt=ds)

    city_sql = '''
            select 
            city_name,
            nvl(round(sum(push_order_times_num)/sum(push_driver_num),2),0) push_driver_times_avg,
            nvl(round(sum(push_order_num)/sum(push_driver_num),2),0) push_driver_order_avg,
            nvl(round(sum(driver_onlinerange_sum)/3600,2),0) driver_onlinerange_sum,
            nvl(round(sum(driver_onlinerange_sum)/(3600 * sum(onride_driver_num)),2),0) driver_onlinerange_rate,
            concat(cast(nvl(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2),0) as string),'%') duration_rate,
            nvl(round(sum(onride_num)/sum(onride_driver_num),2),0) onride_driver_order_avg,
            nvl(sum(online_driver_num),0) online_driver_num,
            nvl(sum(accept_driver_num),0) accept_driver_num,
            nvl(sum(onride_driver_num),0) onride_driver_num,
            nvl(sum(register_driver_num),0) register_driver_num,
            nvl(sum(register_and_onride_driver_num),0) register_and_onride_driver_num,
            nvl(sum(agg_register_driver_num),0) agg_register_driver_num,
            nvl(sum(agg_onride_driver_num),0) agg_onride_driver_num,
            nvl(round(sum(price_sum)/sum(order_pay_num),2),0) order_price_avg,
            nvl(round(sum(amount_sum)/sum(order_pay_num),2),0) order_amount_avg
            from 
            oride_bi.oride_all_driver_capacity_metrics_info
            where dt = '{dt}' and serv_type = 3
            group by city_name
            order by city_name

    '''.format(dt=ds)

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
                    background-color: #CCE0F1;
                    //color: white;
                    width: 100px;
                }}
            </style>
            </head>
            <body>
                <table width="100%" class="table">
                    <caption>
                        <h3>Otrike指标数据</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="2" style="text-align: center;">总需求</th>
                            <th colspan="7" style="text-align: center;">总供给</th>
                            <th colspan="4" style="text-align: center;">招募</th>
                            <th colspan="2" style="text-align: center;">财务</th>
                        </tr>
                        <tr>
                            <th>城市</th>
                            <!--总需求-->
                            <th>人均推单次数</th>
                            <th>人均推送订单数</th>
                            <!--总供给-->
                            <th>总在线时长</th>
                            <th>人均在线时长</th>
                            <th>计费时长占比</th>
                            <th>人均完单数</th>
                            <th>在线司机数</th>
                            <th>接单司机数</th>
                            <th>完单司机数</th>
                            <!--招募-->
                            <th>注册司机数</th>
                            <th>注册且完单司机数</th>
                            <th>累计注册司机数</th>
                            <th>累计完单司机数</th>

                            <!--财务-->
                            <th>单均应付</th>
                            <th>单均实付</th>
                            <!--司机考核-->

                        </tr>
                    </thead>
                    {rows}
                </table>
            </body>
            </html>
            '''

    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()

    logging.info(city_sql)
    cursor.execute(city_sql)
    city_res = cursor.fetchall()

    row_html = ''
    tr_fmt = '''
                <tr>{row}</tr>
            '''

    row_fmt = '''
                <th>{city_name}</th>
                <th>{order_time_push_driver_avg}</th>
                <th>{order_push_driver_avg}</th>
                <th>{driver_online_time_sum}</th>
                <th>{driver_online_avg}</th>
                <th>{duration_rate}</th>
                <th>{onride_avg}</th>
                <th>{online_driver_num}</th>
                <th>{accpet_driver_num}</th>
                <th>{onride_driver_num}</th>
                <th>{register_driver_num}</th>
                <th>{register_and_onride_driver_num}</th>
                <th>{agg_register_driver_num}</th>
                <th>{agg_onride_driver_num}</th>
                <th>{price_avg}</th>
                <th>{amount_avg}</th>
        '''

    for data in res:
        [
            city_name,
            order_time_push_driver_avg,
            order_push_driver_avg,
            driver_online_time_sum,
            driver_online_avg,
            duration_rate,
            onride_avg,
            online_driver_num,
            accpet_driver_num,
            onride_driver_num,
            register_driver_num,
            register_and_onride_driver_num,
            agg_register_driver_num,
            agg_onride_driver_num,
            price_avg,
            amount_avg
        ] = data

        row = row_fmt.format(
            city_name=city_name,
            order_time_push_driver_avg=order_time_push_driver_avg,
            order_push_driver_avg=order_push_driver_avg,
            driver_online_time_sum=driver_online_time_sum,
            driver_online_avg=driver_online_avg,
            duration_rate=duration_rate,
            onride_avg=onride_avg,
            online_driver_num=online_driver_num,
            accpet_driver_num=accpet_driver_num,
            onride_driver_num=onride_driver_num,
            register_driver_num=register_driver_num,
            register_and_onride_driver_num=register_and_onride_driver_num,
            agg_register_driver_num=agg_register_driver_num,
            agg_onride_driver_num=agg_onride_driver_num,
            price_avg=price_avg,
            amount_avg=amount_avg
        )

        row_html += tr_fmt.format(row=row)

    for data in city_res:
        [
            city_name,
            order_time_push_driver_avg,
            order_push_driver_avg,
            driver_online_time_sum,
            driver_online_avg,
            duration_rate,
            onride_avg,
            online_driver_num,
            accpet_driver_num,
            onride_driver_num,
            register_driver_num,
            register_and_onride_driver_num,
            agg_register_driver_num,
            agg_onride_driver_num,
            price_avg,
            amount_avg
        ] = data

        row = row_fmt.format(
            city_name=city_name,
            order_time_push_driver_avg=order_time_push_driver_avg,
            order_push_driver_avg=order_push_driver_avg,
            driver_online_time_sum=driver_online_time_sum,
            driver_online_avg=driver_online_avg,
            duration_rate=duration_rate,
            onride_avg=onride_avg,
            online_driver_num=online_driver_num,
            accpet_driver_num=accpet_driver_num,
            onride_driver_num=onride_driver_num,
            register_driver_num=register_driver_num,
            register_and_onride_driver_num=register_and_onride_driver_num,
            agg_register_driver_num=agg_register_driver_num,
            agg_onride_driver_num=agg_onride_driver_num,
            price_avg=price_avg,
            amount_avg=amount_avg
        )

        row_html += tr_fmt.format(row=row)

    html = html_fmt.format(rows=row_html, dt=ds)

    # send mail

    email_to = Variable.get("oride_otrike_driver_transport_metrics_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']
        # email_to = ['nan.li@opay-inc.com']

    email_subject = '司机运力日报-Otrike_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_otrike_report = PythonOperator(
    task_id='send_otrike_report',
    python_callable=send_otrike_report_email,
    provide_context=True,
    dag=dag
)

validate_partition_data >> data_driver_extend_validate_task >> insert_driver_metrics
validate_partition_data >> data_city_conf_validate_task >> insert_driver_metrics
validate_partition_data >> data_order_payment_validate_task >> insert_driver_metrics
validate_partition_data >> data_order_validate_task >> insert_driver_metrics
validate_partition_data >> server_magic_push_detail_validate_task >> insert_driver_metrics
validate_partition_data >> oride_driver_timerange_validate_task >> insert_driver_metrics

insert_driver_metrics >> send_fast_report
insert_driver_metrics >> send_otrike_report

