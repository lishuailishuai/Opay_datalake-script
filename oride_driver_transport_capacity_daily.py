import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.email import send_email
import logging
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 6, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_driver_transport_capacity_daily',
    schedule_interval="00 02 * * *",
    default_args=args)

city_list = [
    'ALL',
    'lagos',
    'Ibadan'
]

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
            e.register_time
            from 
            oride_db.data_driver_extend e
            join oride_db.data_city_conf c on c.dt = '{{ ds }}' and e.city_id = c.id and c.name <> 'beijing'
            where e.dt = '{{ ds }}' and e.serv_type = 2
        ),


         online_data as (
            select 
            d.city_name city_name,
            count(d.id) online_driver_num, -- 在线司机数
            sum(if(t.driver_onlinerange is null,0,t.driver_onlinerange)) driver_onlinerange_sum -- 在线时长
            from 
            driver_dim d 
            join oride_bi.oride_driver_timerange t on d.id = t.driver_id and dt = '{{ ds }}'
            group by d.city_name
        ),


        order_data as (
                select 
                d.city_name ,
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' ,do.id,null)) accept_num, --当日接单量
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' ,do.driver_id,null)) accept_driver_num, --当日接单司机数
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.id,null)) onride_num, -- 当日完单量
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.driver_id,null)) onride_driver_num, -- 完单司机数
                count(distinct if(do.status = 4 or do.status = 5,do.driver_id,null)) agg_onride_driver_num, -- 累计完单司机数
                count(distinct if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and from_unixtime(d.register_time,'yyyy-MM-dd') = '{{ ds }}' and (do.status = 4 or do.status = 5),do.driver_id,null)) register_and_onride_driver_num, -- 当日注册且完单司机数
                sum(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}',duration,0))  duration_sum, -- 计费时长
                count(if(from_unixtime(do.create_time,'yyyy-MM-dd') = '{{ ds }}' and do.status = 6 and do.cancel_role = 2,do.id,null)) driver_cancel_num --司机取消订单数
                from
                oride_db.data_order do
                join driver_dim d on do.driver_id = d.id
                where dt = '{{ ds }}'
                group by d.city_name
        ),


        order_pay as (
            select 
            d.city_name city_name,
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
            group by d.city_name
        ),




        driver_register as (
            select 
            city_name,
            count(if(from_unixtime(register_time,'yyyy-MM-dd') = '{{ ds }}',id,null)) register_driver_num, --注册司机数
            count(id) agg_register_driver_num  -- 累计注册司机数
            from 
            driver_dim 
            group by city_name
        ),


        order_push as (
            select 
            o.city_name,
            count(s.order_id) push_order_times_num, --推送订单次数
            count(distinct(s.order_id)) push_order_num -- 成功推送订单量
            from 
            (
                select 
                order_id,
                driver_id
                from oride_bi.server_magic_push_detail
                where dt = '{{ ds }}' and success = 1
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
            group by o.city_name
        ),
        
        
        order_push_driver as (
            select 
            o.city_name,
            count(driver_num)  push_driver_num, -- 推送司机数
            sum(driver_num) push_order_to_driver_num -- 订单推送司机数
            from 
            (
                select 
                t.order_id,
                max(t.driver_num) driver_num
                from 
                (
                select 
                order_id,
                round,
                count(driver_id) driver_num
                from oride_bi.server_magic_push_detail
                where dt = '{{ ds }}' and success = 1
                group by order_id,round
                ) t
                group by t.order_id
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
            group by o.city_name
        )


        insert overwrite table oride_driver_capacity_metrics_info partition (dt='{{ ds }}')
        select 
        od.city_name,
        opd.push_order_to_driver_num,
        opd.push_driver_num,
        opu.push_order_times_num,
        opu.push_order_num,
        oda.online_driver_num,
        oda.driver_onlinerange_sum,
        od.duration_sum,
        od.onride_num,
        od.accept_driver_num,
        od.onride_driver_num,
        od.accept_num,
        od.register_and_onride_driver_num,
        od.agg_onride_driver_num,
        od.driver_cancel_num,
        dr.register_driver_num,
        dr.agg_register_driver_num,
        op.order_pay_num,
        op.price_sum,
        op.amount_sum

        from 
        order_data od 
        left join online_data oda on od.city_name = oda.city_name
        left join order_pay op on od.city_name = op.city_name
        left join driver_register dr on od.city_name = dr.city_name 
        left join order_push opu on od.city_name = opu.city_name
        left join order_push_driver opd on od.city_name = opd.city_name
        ;


        ''',
    schema='oride_bi',
    dag=dag)


def send_report_email(ds, **kwargs):
    cursor = get_hive_cursor()
    sql = '''
        
        select 
        t.*
        
        from 
        (
            select 
            city_name,
            round(sum(push_order_to_driver_num)/sum(push_driver_num),2),
            round(sum(push_order_times_num)/sum(online_driver_num),2),
            round(sum(push_order_num)/sum(online_driver_num),2),
            round(sum(driver_onlinerange_sum)/3600,2),
            round(sum(driver_onlinerange_sum)/(3600 * sum(online_driver_num)),2),
            concat(cast(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2) as string),'%'),
            round(sum(onride_num)/sum(onride_driver_num),2),
            sum(online_driver_num),
            sum(accept_driver_num),
            sum(onride_driver_num),
            sum(register_driver_num),
            sum(register_and_onride_driver_num),
            sum(agg_register_driver_num),
            sum(agg_onride_driver_num),
            round(sum(price_sum)/sum(order_pay_num),2),
            round(sum(amount_sum)/sum(order_pay_num),2),
            concat(cast(round((sum(driver_cancel_num) * 100)/sum(accept_num),2) as string),'%')
            from 
            oride_bi.oride_driver_capacity_metrics_info
            where dt = '{dt}'
            group by city_name
            union all
            select 
            'ALL' city_name,
            round(sum(push_order_to_driver_num)/sum(push_driver_num),2),
            round(sum(push_order_times_num)/sum(online_driver_num),2),
            round(sum(push_order_num)/sum(online_driver_num),2),
            round(sum(driver_onlinerange_sum)/3600,2),
            round(sum(driver_onlinerange_sum)/(3600 * sum(online_driver_num)),2),
            concat(cast(round((sum(duration_sum) * 100)/sum(driver_onlinerange_sum),2) as string),'%'),
            round(sum(onride_num)/sum(onride_driver_num),2),
            sum(online_driver_num),
            sum(accept_driver_num),
            sum(onride_driver_num),
            sum(register_driver_num),
            sum(register_and_onride_driver_num),
            sum(agg_register_driver_num),
            sum(agg_onride_driver_num),
            round(sum(price_sum)/sum(order_pay_num),2),
            round(sum(amount_sum)/sum(order_pay_num),2),
            concat(cast(round((sum(driver_cancel_num) * 100)/sum(accept_num),2) as string),'%')
            from 
            oride_bi.oride_driver_capacity_metrics_info
            where dt = '{dt}'
        ) t 
        order by t.city_name
        

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
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="3" style="text-align: center;">总需求</th>
                            <th colspan="7" style="text-align: center;">总供给</th>
                            <th colspan="4" style="text-align: center;">招募</th>
                            <th colspan="2" style="text-align: center;">财务</th>
                            <th colspan="1" style="text-align: center;">司机考核</th>
                        </tr>
                        <tr>
                            <th>城市</th>
                            <!--总需求-->
                            <th>单均推送司机数</th>
                            <th>人均推单次数</th>
                            <th>人均成功推单量</th>
                            <!--总供给-->
                            <th>总在线时长</th>
                            <th>人均在线时长</th>
                            <th>计费时长占比</th>
                            <th>人均完单量</th>
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
                            <th>司机取消率</th>

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

    row_html = ''
    tr_fmt = '''
                <tr>{row}</tr>
            '''

    row_fmt = '''
                <th>{city_name}</th>
                <th>{order_dispatch_push_driver_avg}</th>
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
                <th>{driver_cancel_rate}</th>
        '''

    data_map = dict()
    for data in res:
        data_list = list(data)
        data_map[data_list[0]] = data_list

    for city in city_list:
        data = data_map.get(city)

        [
            city_name,
            order_dispatch_push_driver_avg,
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
            amount_avg,
            driver_cancel_rate
        ] = data

        row = row_fmt.format(
            city_name=city_name,
            order_dispatch_push_driver_avg=order_dispatch_push_driver_avg,
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
            amount_avg=amount_avg,
            driver_cancel_rate=driver_cancel_rate
        )

        row_html += tr_fmt.format(row=row)

    html = html_fmt.format(rows=row_html, dt=ds)

    # send mail
    email_subject = '司机运力日报-快车_{}'.format(ds)
    send_email(
        Variable.get("oride_driver_transport_metrics_receivers").split()
        # ['nan.li@opay-inc.com']
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

insert_driver_metrics >> send_report
