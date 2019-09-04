import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_hive_cursor, get_db_conf
from airflow.operators.hive_operator import HiveOperator
import logging
from airflow.models import Variable

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 6, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_night_report',
    schedule_interval="0 22 * * *",
    default_args=args)

table_list = [
    "data_order",
    "data_order_payment",
    "data_user",
    "data_user_extend",
    "data_driver_extend",
    "data_user_recharge",
    "data_driver_recharge_records",
    "data_driver_reward",
]


def send_report_email(tomorrow_ds, ds, **kwargs):
    sql = '''
        with user_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as new_users,
                sum(if(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) as active_users
            FROM
                oride_db.data_user_extend
            WHERE
                dt='{ds}'
            GROUP BY dt
        ),
        driver_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as new_drivers,
                count(id) as total_drivers
            FROM
                oride_db.data_driver_extend
            WHERE
                dt='{ds}'
            GROUP BY dt
        ),
        order_data as (
            SELECT
                do.dt,
                COUNT(DISTINCT do.user_id) as request_users,
                COUNT(DISTINCT if(dop.mode=2 or dop.mode=3, do.user_id, NULL)) as online_pay_users,
                COUNT(DISTINCT if(do.driver_id>0, do.driver_id, NULL)) as take_drivers,
                COUNT(DISTINCT if(do.status=4 or do.status=5, do.driver_id, NULL)) as finish_drivers,
                COUNT(do.id) as request_num,
                SUM(if(do.driver_id>0, 1, 0)) as take_num,
                SUM(if(do.status=4 or do.status=5, 1, 0)) as finish_num,
                COUNT(DISTINCT if(dop.mode=2 or dop.mode=3, do.id, NULL)) as online_pay_orders,
                SUM(if(do.take_time>0, do.take_time-do.create_time, 0)) as total_take_time,
                SUM(if(do.pickup_time>0, do.pickup_time-do.take_time, 0)) as total_pickup_time,
                SUM(if(do.pickup_time>0, 1, 0)) as pickup_num
            FROM
                oride_db.data_order do
                LEFT JOIN oride_db.data_order_payment dop ON dop.id=do.id AND dop.dt=do.dt
            WHERE
                do.dt='{ds}' AND from_unixtime(do.create_time, 'yyyy-MM-dd')=do.dt
            GROUP BY do.dt
        ),
        recharge_data as (
            SELECT
                dt,
                count(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, id, null)) as td_recharge_times,
                sum(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, amount, 0)) as td_recharge_amount,
                count(id) as recharge_times,
                sum(amount) as recharge_amount,
                sum(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, bonus, 0)) as td_recharge_bonus,
                sum(bonus) as recharge_bonus
            FROM
                oride_db.data_user_recharge
            WHERE
                dt='{ds}' AND status=1
            GROUP BY dt
        )
        SELECT
            od.request_users,
            ud.active_users,
            ud.new_users,
            od.online_pay_users,
            dd.total_drivers,
            dd.new_drivers,
            od.take_drivers,
            od.finish_drivers,
            od.request_num,
            od.take_num,
            od.finish_num,
            od.online_pay_orders,
            od.total_take_time,
            od.total_pickup_time,
            od.pickup_num,
            rd.td_recharge_times,
            rd.td_recharge_amount,
            rd.recharge_times,
            rd.recharge_amount,
            rd.td_recharge_bonus,
            rd.recharge_bonus
        FROM
            user_data ud
            INNER JOIN driver_data dd ON dd.dt=ud.dt
            INNER JOIN order_data od ON od.dt=ud.dt
            LEFT JOIN recharge_data rd on rd.dt=ud.dt
    '''.format(ds=tomorrow_ds)
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    if len(res) > 0:
        [request_users, active_users, new_users, online_pay_users, total_drivers, new_drivers, take_drivers,
         finish_drivers, request_num, take_num, finish_num, online_pay_orders, total_take_time, total_pickup_time,
         pickup_num, td_recharge_times, td_recharge_amount, recharge_times, recharge_amount, td_recharge_bonus,
         recharge_bonus] = list(res[0])
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
            }}
            table td, table th
            {{
                border: 1px solid #cad9ea;
                color: #666;
                height: 30px;
                padding: 5px 10px 5px 5px;
            }}
            table thead th
            {{
                background-color: #4CAF50;
                color: white;
                width: 100px;
            }}
            table tr:nth-child(odd)
            {{
                background: #fff;
            }}
            table tr:nth-child(even)
            {{
                background: #F5FAFA;
            }}
        </style>
        </head>
        <body>
            <table width="95%" class="table">
                <caption>
                    <h2>{dt}</h2>
                </caption>
            </table>
            <table width="95%" class="table">
                <thead>
                    <tr>
                        <th colspan="4" style="text-align: center;">乘客</th>
                        <th colspan="5" style="text-align: center;">司机</th>
                        <th colspan="8" style="text-align: center;">订单</th>
                        <th colspan="6" style="text-align: center;">充值</th>
                    </tr>
                    <tr>
                        <!--乘客-->
                        <th>发单乘客数</th>
                        <th>活跃乘客数</th>
                        <th>新增活跃乘客数</th>
                        <th>线上支付乘客数</th>
                        <!--司机-->
                        <th>累计注册司机数</th>
                        <th>注册司机数</th>
                        <th>抢单司机数</th>
                        <th>完单司机数</th>
                        <th>人均完单数</th>
                        <!--订单-->
                        <th>下单数</th>
                        <th>抢单数</th>
                        <th>完单数</th>
                        <th>应答率</th>
                        <th>完单率</th>
                        <th>线上支付订单数</th>
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <!--充值-->
                        <th>每日充值笔数</th>
                        <th>累计充值笔数</th>
                        <th>每日充值真实金额</th>
                        <th>累计充值真实金额</th>
                        <th>每日充值总金额</th>
                        <th>累计充值总金额</th>
                    </tr>
                </thead>
                <tr>
                    <!--乘客-->
                    <td>{request_users}</td>
                    <td>{active_users}</td>
                    <td>{new_users}</td>
                    <td>{online_pay_users}</td>
                    <!--司机-->
                    <td>{total_drivers}</td>
                    <td>{new_drivers}</td>
                    <td>{take_drivers}</td>
                    <td>{finish_drivers}</td>
                    <td>{finish_vs_driver}</td>
                    <!--订单-->
                    <td>{request_num}</td>
                    <td>{take_num}</td>
                    <td>{finish_num}</td>
                    <td>{take_ratio}%</td>
                    <td>{finish_ratio}%</td>
                    <td>{online_pay_orders}</td>
                    <td>{take_time_avg}</td>
                    <td>{pickup_time_avg}</td>
                    <!--充值-->
                    <td>{td_recharge_times}</td>
                    <td>{recharge_times}</td>
                    <td>{td_recharge_amount}</td>
                    <td>{recharge_amount}</td>
                    <td>{td_recharge_total}</td>
                    <td>{recharge_total}</td>
                </tr>
            </table>
            
            
            
            <table width="95%" class="table">
                <caption>
                    <h2>{dt}</h2>
                </caption>
            </table>
            <table width="95%" class="table">
                <thead>
                    <tr>
                        <th>城市</th>
                        <th>完单数</th>
                        <th>gmv</th>
                        <th>单均应付</th>
                        <th>单均应付对比昨日</th>
                        <th>总补贴率</th>
                        <th>单均补贴</th>
                        <th>单均补贴对比昨日</th>
                        <th>b端补贴</th>
                        <th>b端补贴率</th>
                        <th>b端单均补贴</th>
                        <th>c端补贴</th>
                        <th>c端补贴率</th>
                        <th>c端单均补贴</th>
                        <!-- <th>平台抽成</th> -->
                    </tr>
                </thead>
                {rows}
            </table>
            
        </body>
        </html>
        '''

        rows = get_city_data(ds, tomorrow_ds)

        html = html_fmt.format(
            dt=tomorrow_ds,
            request_users=request_users,
            active_users=active_users,
            new_users=new_users,
            online_pay_users=online_pay_users,
            total_drivers=total_drivers,
            new_drivers=new_drivers,
            take_drivers=take_drivers,
            finish_drivers=finish_drivers,
            finish_vs_driver=round(finish_num / finish_drivers, 2),
            request_num=request_num,
            take_num=take_num,
            finish_num=finish_num,
            take_ratio=round(take_num / request_num * 100, 2),
            finish_ratio=round(finish_num / request_num * 100, 2),
            online_pay_orders=online_pay_orders,
            take_time_avg=round(total_take_time / take_num),
            pickup_time_avg=round(total_pickup_time / pickup_num),
            td_recharge_times=td_recharge_times,
            td_recharge_amount=td_recharge_amount,
            recharge_times=recharge_times,
            recharge_amount=recharge_amount,
            td_recharge_total=td_recharge_amount + td_recharge_bonus,
            recharge_total=recharge_amount + recharge_bonus,
            rows=rows
        )
        # send mail
        email_subject = 'oride晚十点数据快报_{}'.format(tomorrow_ds)
        send_email(
            Variable.get("oride_night_report_receivers").split()
            , email_subject, html, mime_charset='utf-8')
        # send_email(['zhenqian.zhang@opay-inc.com'], email_subject, html, mime_charset='utf-8')
        cursor.close()
        return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

insert_city_metrics = HiveOperator(
    task_id='insert_city_metrics',
    hql=''' 
    
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table oride_bi.oride_night_city_metrics_report partition (dt)
        select 
        o.city_id,
        count(if(o.status in (4,5),o.id,null)) as finish_order_cnt,
        sum(if(o.status in (4,5),p.price,0)) as gmv,
        
        sum(if(o.status in (4,5) ,nvl(r.amount,0) + nvl(d.amount,0),0)) as b_subsidy,
        sum(if(o.status in (4,5),p.price - p.amount,0)) as c_subsidy,
        from_unixtime(o.create_time,'yyyy-MM-dd') dt
        
        -- gmv ods_binlog_data_order_payment_hi price
        --B端 ods_binlog_data_driver_reward_hi ods_binlog_data_driver_recharge_records_hi amount
        --C端 pay_price - pay_amount
        
        from 
        (
            select
            *
            from 
            oride_db.data_order 
            where dt = '{{ tomorrow_ds }}'
            and from_unixtime(create_time,'yyyy-MM-dd') = '{{ tomorrow_ds }}'
        ) o 
        left join 
        (   
            select 
            order_id,
            amount
            from 
            oride_db.data_driver_recharge_records
            where dt = '{{ tomorrow_ds }}'
            and amount>0
        ) r on o.id = r.order_id
        left join 
        (
            select 
            order_id,
            amount
            from 
            oride_db.data_driver_reward
            where dt = '{{ tomorrow_ds }}'
        ) d on o.id = d.order_id
        left join 
        (
            select 
            id,
            price,
            amount
            from 
            oride_db.data_order_payment
            where dt = '{{ tomorrow_ds }}'
        ) p on o.id = p.id
        
        group by 
        from_unixtime(o.create_time,'yyyy-MM-dd'),o.city_id
        ;

        ''',
    schema='oride_bi',
    dag=dag)


def get_city_data(yesterday, day):
    sql = '''
        
        select 
            cur.dt,
            'All',
            sum(cur.finish_order_cnt), --完单数
            sum(cur.gmv), --gmv
            round(nvl(sum(cur.gmv)/sum(cur.finish_order_cnt),0),1) as price_avg, --单均应付
            concat(cast(round(nvl((sum(cur.gmv)/sum(cur.finish_order_cnt)) * 100/
            (sum(yesterday.gmv)/sum(yesterday.finish_order_cnt)),0),1) as string),'%') as price_avg_compare, --单均应付对比昨日
            concat(cast(round(nvl((sum(cur.b_subsidy + cur.c_subsidy)) * 100 / sum(cur.gmv),0),1) as string),'%') as subsidy_rate , --总补贴率
            round(nvl((sum(cur.b_subsidy + cur.c_subsidy))  / sum(cur.finish_order_cnt),0),1) as subsidy_avg, --单均补贴
            concat(cast(round(nvl(((sum(cur.b_subsidy + cur.c_subsidy))  / sum(cur.finish_order_cnt)) * 100 /
            ((sum(yesterday.b_subsidy + yesterday.c_subsidy))  / sum(yesterday.finish_order_cnt)),0),1) as string),'%') as subsidy_avg_compare, -- 单均补贴对比昨日
            
            sum(cur.b_subsidy) as b_subsidy, --b端补贴
            concat(cast(round(nvl(sum(cur.b_subsidy) * 100/ sum(cur.gmv),0),1) as string),'%') as b_subsidy_rate, --b端补贴率
            round(nvl(sum(cur.b_subsidy) / sum(cur.finish_order_cnt),0),1) as b_subsidy_avg, --b端单均补贴
            
            sum(cur.c_subsidy) as c_subsidy, --c端补贴
            concat(cast(round(nvl(sum(cur.c_subsidy) * 100 / sum(cur.gmv),0),1) as string),'%') as c_subsidy_rate , --c端补贴率
            round(nvl(sum(cur.c_subsidy) / sum(cur.finish_order_cnt),0),1) as c_subsidy_avg, --c端单均补贴
            
            round(sum(cur.gmv) * 0.05,1) as platform_money
        from 
        
        (    
            select 
            dt,
            city_id,
            finish_order_cnt,
            gmv,
            b_subsidy,
            c_subsidy 
            from 
            oride_bi.oride_night_city_metrics_report
            where dt = '{day}'
        ) cur
        left join (
            select 
            city_id,
            finish_order_cnt,
            gmv,
            b_subsidy,
            c_subsidy 
            from 
            oride_bi.oride_night_city_metrics_report
            where dt = '{yesterday}'
        ) yesterday on cur.city_id = yesterday.city_id
        group by cur.dt
        union all 
        select 
            cur.dt,
            c.name,
            sum(cur.finish_order_cnt), --完单数
            sum(cur.gmv), --gmv
            round(nvl(sum(cur.gmv)/sum(cur.finish_order_cnt),0),1) as price_avg, --单均应付
            concat(cast(round(nvl((sum(cur.gmv)/sum(cur.finish_order_cnt)) * 100/
            (sum(yesterday.gmv)/sum(yesterday.finish_order_cnt)),0),1) as string),'%') as price_avg_compare, --单均应付对比昨日
            concat(cast(round(nvl((sum(cur.b_subsidy + cur.c_subsidy)) * 100 / sum(cur.gmv),0),1) as string),'%') as subsidy_rate , --总补贴率
            round(nvl((sum(cur.b_subsidy + cur.c_subsidy))  / sum(cur.finish_order_cnt),0),1) as subsidy_avg, --单均补贴
            concat(cast(round(nvl(((sum(cur.b_subsidy + cur.c_subsidy))  / sum(cur.finish_order_cnt)) * 100 /
            ((sum(yesterday.b_subsidy + yesterday.c_subsidy))  / sum(yesterday.finish_order_cnt)),0),1) as string),'%') as subsidy_avg_compare, -- 单均补贴对比昨日
            
            sum(cur.b_subsidy) as b_subsidy, --b端补贴
            concat(cast(round(nvl(sum(cur.b_subsidy) * 100/ sum(cur.gmv),0),1) as string),'%') as b_subsidy_rate, --b端补贴率
            round(nvl(sum(cur.b_subsidy) / sum(cur.finish_order_cnt),0),1) as b_subsidy_avg, --b端单均补贴
            
            sum(cur.c_subsidy) as c_subsidy, --c端补贴
            concat(cast(round(nvl(sum(cur.c_subsidy) * 100 / sum(cur.gmv),0),1) as string),'%') as c_subsidy_rate , --c端补贴率
            round(nvl(sum(cur.c_subsidy) / sum(cur.finish_order_cnt),0),1) as c_subsidy_avg, --c端单均补贴
            
            round(sum(cur.gmv) * 0.05,1) as platform_money
        
        from 
        
        (    
            select 
            dt,
            city_id,
            finish_order_cnt,
            gmv,
            b_subsidy,
            c_subsidy 
            from 
            oride_bi.oride_night_city_metrics_report
            where dt = '{day}'
        ) cur
        join (
            select 
            id,
            name
            from 
            oride_db.data_city_conf 
            where dt = '{yesterday}'
            and id != 999001
        ) c on c.id = cur.city_id
        left join (
            select 
            city_id,
            finish_order_cnt,
            gmv,
            b_subsidy,
            c_subsidy 
            from 
            oride_bi.oride_night_city_metrics_report
            where dt = '{yesterday}'
        ) yesterday on cur.city_id = yesterday.city_id
        group by cur.dt,c.name
    
    '''.format(yesterday=yesterday, day=day)

    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()

    row_html = ''
    if len(res) > 0:
        # [
        #     finish_order_cnt,
        #     gmv,
        #     price_avg,
        #     price_avg_compare,
        #     subsidy_rate,
        #     subsidy_avg,
        #     subsidy_avg_compare,
        #     b_subsidy,
        #     b_subsidy_rate,
        #     b_subsidy_avg,
        #     c_subsidy,
        #     c_subsidy_rate,
        #     c_subsidy_avg,
        #     platform_money
        #
        # ] = list(res[0])

        tr_fmt = '''
               <tr>{row}</tr>
            '''
        row_fmt = '''
                <!--{}-->
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
                <td>{}</td>
                <td>{}</td>
                <td>{}</td>
                <!--<td>{}</td>-->
                
            '''

        for data in res:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
        return row_html


host, port, schema, login, password = get_db_conf('sqoop_db')
for table_name in table_list:
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/oride/db/{table}/dt={{{{ tomorrow_ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(host=host, port=port, schema=schema, username=login, password=password, table=table_name),
        dag=dag,
    )
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table_name),
        hql='''
            ALTER TABLE oride_db.{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ tomorrow_ds }}}}')
        '''.format(table=table_name),
        schema='oride_db',
        dag=dag)

    import_table >> add_partitions >> insert_city_metrics >> send_report
