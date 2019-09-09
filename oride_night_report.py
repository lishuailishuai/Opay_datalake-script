# -*- coding: utf-8 -*-
"""
oride晚10点报表
"""
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_hive_cursor, get_db_conf, get_db_conn
from airflow.operators.hive_operator import HiveOperator
import logging
from airflow.models import Variable
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate

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
    default_args=args
)

table_list = [
    {"db": "oride_data", "table": "data_order",                     "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_order_payment",             "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_user",                      "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_user_extend",               "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_driver_extend",             "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_user_recharge",             "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_driver_recharge_records",   "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_driver_reward",             "conn": "sqoop_db"},
    {"db": "oride_data", "table": "data_driver_records_day",        "conn": "sqoop_db"},
]

hive_db = 'oride_dw_ods'
hive_table = 'ods_sqoop_{bs}_22clock_df'
s3path = 's3a://opay-bi/obus_dw/ods_sqoop_{bs}_22clock_df'
ods_create_table_hql = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
        {columns}
    )
    PARTITIONED BY (
        `country_code` string COMMENT '二位国家码',
        `dt` string comment '日期'
    )
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{s3path}'
'''

# mysql数据类型 与 hive数据类型影射
mysql_type_to_hive = {
    "TINYINT":      "INT",
    "SMALLINT":     "INT",
    "MEDIUMINT":    "INT",
    "INT":          "INT",
    "INTEGER":      "INT",
    "BIGINT":       "BIGINT",
    "FLOAT":        "FLOAT",
    "DOUBLE":       "DOUBLE",
    "DECIMAL":      "DECIMAL(38,2)"
}


# 创建 或 更新hive 表元数据
def create_hive_external_table(db, table, conn, **op_kwargs):

    sqoop_schema = SqoopSchemaUpdate()
    response = sqoop_schema.update_hive_schema(
        hive_db=hive_db,
        hive_table=hive_table.format(bs=table),
        mysql_db=db,
        mysql_table=table,
        mysql_conn=conn
    )
    if response:
        return True

    mysql_conn = get_db_conn(conn)
    mcursor = mysql_conn.cursor()
    sql = '''
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            COLUMN_COMMENT,
            COLUMN_TYPE 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA='{db}' and 
            TABLE_NAME='{table}' 
        ORDER BY ORDINAL_POSITION
    '''.format(db=db, table=table)
    # logging.info(sql)
    mcursor.execute(sql)
    res = mcursor.fetchall()
    # logging.info(res)
    columns = []
    for (name, type, comment, co_type) in res:
        columns.append("`%s` %s comment '%s'" % (name, mysql_type_to_hive.get(type.upper(), 'string'), comment))

    # 创建hive数据表的sql
    hql = ods_create_table_hql.format(
        db_name=hive_db,
        table_name=hive_table.format(bs=table),
        columns=",\n".join(columns),
        s3path=s3path.format(bs=table)
    )
    # logging.info(hql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(hql)
    mcursor.close()
    hive_cursor.close()


# 发送邮件报表 ods_sqoop_{bs}_22clock_df
def send_report_email(tomorrow_ds, ds, **kwargs):
    sql = '''
        WITH 
        user_data AS (
            SELECT
                dt,
                SUM(IF(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) AS new_users,      --新增用户数
                SUM(IF(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) AS active_users       --活跃用户数
            FROM
                oride_dw_ods.ods_sqoop_data_user_extend_22clock_df 
            WHERE
                dt = '{ds}' 
            GROUP BY dt 
        ),
        driver_data AS (
            SELECT
                dt,
                SUM(IF(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) AS new_drivers,    --新注册司机数
                COUNT(id) AS total_drivers      --累计司机数
            FROM
                oride_dw_ods.ods_sqoop_data_driver_extend_22clock_df 
            WHERE
                dt = '{ds}' 
            GROUP BY dt 
        ),
        order_data AS (
            SELECT
                do.dt,
                COUNT(DISTINCT do.user_id) AS request_users,                                            --下单用户数
                COUNT(DISTINCT IF(dop.mode=2 OR dop.mode=3, do.user_id, NULL)) AS online_pay_users,     --线上支付用户数
                COUNT(DISTINCT IF(do.driver_id>0, do.driver_id, NULL)) AS take_drivers,                 --接单司机数
                COUNT(DISTINCT IF(do.status=4 OR do.status=5, do.driver_id, NULL)) AS finish_drivers,   --完单司机数
                COUNT(do.id) AS request_num,                                                            --下单数
                SUM(IF(do.driver_id>0, 1, 0)) AS take_num,                                              --接单数
                SUM(IF(do.status=4 OR do.status=5, 1, 0)) AS finish_num,                                --完单数
                COUNT(DISTINCT IF(dop.mode=2 OR dop.mode=3, do.id, NULL)) AS online_pay_orders,         --线上支付订单数
                SUM(IF(do.take_time>0, do.take_time-do.create_time, 0)) AS total_take_time,             --总应单时长(秒)
                SUM(IF(do.pickup_time>0, do.pickup_time-do.take_time, 0)) AS total_pickup_time,         --总接驾时长(秒)
                SUM(IF(do.pickup_time>0, 1, 0)) AS pickup_num                                           --成功接驾订单数
            FROM (SELECT 
                    * 
                FROM oride_dw_ods.ods_sqoop_data_order_22clock_df 
                WHERE dt = '{ds}' AND 
                    from_unixtime(create_time, 'yyyy-MM-dd') = dt 
                ) AS do 
            LEFT JOIN (SELECT 
                    * 
                FROM oride_dw_ods.ods_sqoop_data_order_payment_22clock_df 
                WHERE dt = '{ds}' 
                ) AS dop 
            ON dop.id = do.id AND 
                dop.dt = do.dt 
            GROUP BY do.dt 
        ),
        recharge_data AS (
            SELECT
                dt,
                COUNT(IF(from_unixtime(create_time, 'yyyy-MM-dd')=dt, id, null)) AS td_recharge_times,      --充值次数
                SUM(IF(from_unixtime(create_time, 'yyyy-MM-dd')=dt, amount, 0)) AS td_recharge_amount,      --充值金额
                COUNT(id) AS recharge_times,                                                                --累计充值次数
                SUM(amount) AS recharge_amount,                                                             --累计充值金额
                SUM(IF(from_unixtime(create_time, 'yyyy-MM-dd')=dt, bonus, 0)) AS td_recharge_bonus,        --奖励金额
                SUM(bonus) AS recharge_bonus                                                                --累计奖励金额
            FROM
                oride_dw_ods.ods_sqoop_data_user_recharge_22clock_df 
            WHERE
                dt = '{ds}' AND 
                status = 1 
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
                        <th>业务线</th>
                        <th>完单数</th>
                        <th>完单数同比上周同期</th>
                        <th>GMV</th>
                        <th>GMV同比上周同期</th>
                        <th>单均应付</th>
                        <th>单均应付同比上周同期</th>
                        <th>总补贴</th>
                        <th>总补贴率</th>
                        <th>单均补贴</th>
                        <th>单均补贴同比上周同期</th>
                        <th>b端补贴</th>
                        <th>b端补贴率</th>
                        <th>b端单均补贴</th>
                        <th>c端补贴</th>
                        <th>c端补贴率</th>
                        <th>c端单均补贴</th>
                        <th>业务毛利</th>
                        <th>业务毛利同比上周同期</th>
                    </tr>
                </thead>
                {rows}
            </table>
            
        </body>
        </html>
        '''

        rows = get_city_data(kwargs.get('ystd'), kwargs.get('db1'), tomorrow_ds)

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
            # 'duo.wu@opay-inc.com'
            , email_subject, html, mime_charset='utf-8'
        )
        # send_email(['zhenqian.zhang@opay-inc.com'], email_subject, html, mime_charset='utf-8')

    cursor.close()
    return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    op_kwargs={
        'ystd': '{{macros.ds_add(ds, -6)}}',
        'db1': '{{ds}}'
    },
    dag=dag
)

# 计算分城市、分业务基础数据
insert_city_metrics = HiveOperator(
    task_id='insert_city_metrics',
    hql=''' 
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        WITH 
        amount_service AS (
            SELECT 
                dr.dt,
                dr.city_id, 
                dr.serv_type, 
                SUM(amount_service) AS amount_service
            FROM (SELECT 
                    dt, 
                    city_id,
                    serv_type,
                    id
                FROM oride_dw_ods.ods_sqoop_data_driver_extend_22clock_df 
                WHERE dt = '{{ tomorrow_ds }}'
                ) AS dr 
            JOIN (SELECT 
                    driver_id,
                    amount_service 
                FROM oride_dw_ods.ods_sqoop_data_driver_records_day_22clock_df 
                WHERE dt = '{{ tomorrow_ds }}' AND 
                    from_unixtime(day, 'yyyy-MM-dd') = '{{ tomorrow_ds }}' 
                ) AS drd 
            ON dr.id = drd.driver_id 
            GROUP BY dr.dt, dr.city_id, dr.serv_type 
        )
        INSERT OVERWRITE TABLE oride_bi.oride_night_city_metrics_report PARTITION (dt) 
        SELECT 
            a.city_id,
            a.finish_order_cnt, 
            a.gmv, 
            a.b_subsidy, 
            a.c_subsidy, 
            a.serv_type, 
            a.b_subsidy2,
            IF(b.amount_service IS NULL, 0, b.amount_service),           --份子
            a.dt 
        FROM (
            SELECT  
                o.city_id,                                                                  --城市ID
                COUNT(1) AS finish_order_cnt,                                               --完单数
                SUM(IF(p.price IS NULL, 0, p.price)) AS gmv,                                --GMV
                SUM(IF(r.amount IS NULL OR r.amount<0,0,r.amount) + IF(d.amount IS NULL,0,d.amount)) AS b_subsidy,  --B端补贴
                SUM(IF(p.price IS NULL,0,p.price) - IF(p.amount IS NULL,0,p.amount)) AS c_subsidy,    --C端补贴 
                o.serv_type,                                                                --业务类型
                SUM(IF(r.amount IS NULL OR r.amount>0, 0, r.amount)) AS b_subsidy2,         --B端扣款
                from_unixtime(o.create_time, 'yyyy-MM-dd') AS dt 
            FROM 
            (
                SELECT 
                    od.create_time, 
                    od.id,
                    dr.city_id, 
                    dr.serv_type 
                FROM (SELECT 
                        create_time, 
                        id,
                        driver_id 
                    FROM 
                        oride_dw_ods.ods_sqoop_data_order_22clock_df 
                    WHERE dt = '{{ tomorrow_ds }}' AND 
                        from_unixtime(create_time, 'yyyy-MM-dd') = '{{ tomorrow_ds }}' AND 
                        status IN (4,5)
                    ) AS od 
                JOIN (SELECT 
                        city_id,
                        serv_type,
                        id
                    FROM oride_dw_ods.ods_sqoop_data_driver_extend_22clock_df 
                    WHERE dt = '{{ tomorrow_ds }}'
                    ) AS dr 
                ON od.driver_id = dr.id 
                ) AS o 
            LEFT JOIN 
                (   
                SELECT 
                    order_id,
                    amount
                FROM 
                    oride_dw_ods.ods_sqoop_data_driver_recharge_records_22clock_df 
                WHERE dt = '{{ tomorrow_ds }}' 
                ) AS r ON o.id = r.order_id 
            LEFT JOIN 
                (
                SELECT 
                    order_id,
                    amount
                FROM 
                    oride_dw_ods.ods_sqoop_data_driver_reward_22clock_df
                WHERE dt = '{{ tomorrow_ds }}' 
                ) AS d ON o.id = d.order_id
            LEFT JOIN 
                (
                SELECT 
                    id,
                    price,
                    amount
                FROM 
                    oride_dw_ods.ods_sqoop_data_order_payment_22clock_df 
                WHERE dt = '{{ tomorrow_ds }}'
                ) AS p ON o.id = p.id 
            GROUP BY 
                from_unixtime(o.create_time,'yyyy-MM-dd'), o.city_id, o.serv_type 
            ) AS a 
        LEFT JOIN amount_service AS b ON a.dt=b.dt AND a.city_id=b.city_id AND a.serv_type=b.serv_type  
        ''',
    schema='oride_bi',
    dag=dag
)


# 获取城市、业务数据
def get_city_data(yesterday, db1, day):
    sql = '''
        --汇总数据
        SELECT 
            cur.dt,
            'All' AS city,
            '-' AS serv_type,
            SUM(cur.finish_order_cnt),      --完单数
            IF(SUM(yesterday.finish_order_cnt)>0, 
                ROUND(SUM(cur.finish_order_cnt)*100/SUM(yesterday.finish_order_cnt),1), 
                '-' 
            ),                          --完单数同比上周同期
            SUM(cur.gmv),   --gmv
            IF(SUM(yesterday.gmv)>0, 
                ROUND(SUM(cur.gmv)*100/SUM(yesterday.gmv),1), 
                '-'
            ),                          --GMV同比上周同期
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.gmv)/SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS price_avg,             --单均应付
            IF(IF(SUM(yesterday.finish_order_cnt)>0, SUM(yesterday.gmv)/SUM(yesterday.finish_order_cnt), 0)>0,
                ROUND(IF(SUM(cur.finish_order_cnt)>0, SUM(cur.gmv)/SUM(cur.finish_order_cnt), 0) * 100 /
                    (SUM(yesterday.gmv)/SUM(yesterday.finish_order_cnt)), 1), 
                '-'
            ) AS price_avg_compare,     --单均应付对比上周同期
            SUM(cur.b_subsidy + cur.c_subsidy),         --总补贴
            IF(SUM(cur.gmv)>0, 
                ROUND((SUM(cur.b_subsidy + cur.c_subsidy)) * 100 / SUM(cur.gmv),1), 
                '-' 
            ) AS subsidy_rate,          --总补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND((SUM(cur.b_subsidy + cur.c_subsidy))  / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS subsidy_avg,           --单均补贴
            IF(IF(SUM(yesterday.finish_order_cnt)>0, (SUM(yesterday.b_subsidy + yesterday.c_subsidy))  / SUM(yesterday.finish_order_cnt), 0)>0,
                ROUND(IF(SUM(cur.finish_order_cnt)>0, (SUM(cur.b_subsidy + cur.c_subsidy))  / SUM(cur.finish_order_cnt), 0) * 100 /
                    ((SUM(yesterday.b_subsidy + yesterday.c_subsidy))  / SUM(yesterday.finish_order_cnt)),1), 
                '-'
            ) AS subsidy_avg_compare,   --单均补贴对比上周同期
            SUM(cur.b_subsidy) AS b_subsidy, --b端补贴
            IF(SUM(cur.gmv)>0, 
                ROUND(SUM(cur.b_subsidy) * 100/ SUM(cur.gmv),1), 
                '-' 
            ) AS b_subsidy_rate,        --b端补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.b_subsidy) / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS b_subsidy_avg,         --b端单均补贴
            SUM(cur.c_subsidy) AS c_subsidy, --c端补贴
            IF(SUM(cur.gmv)>0, 
                ROUND(SUM(cur.c_subsidy) * 100 / SUM(cur.gmv),1), 
                '-' 
            ) AS c_subsidy_rate ,       --c端补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.c_subsidy) / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS c_subsidy_avg,         --c端单均补贴
            SUM(cur.amount_service + cur.b_deduction - cur.b_subsidy - cur.c_subsidy),      --业务毛利  
            IF(SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy)<>0 AND 
                SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy) IS NOT NULL, 
                ROUND(SUM(cur.amount_service+cur.b_deduction-cur.b_subsidy-cur.c_subsidy) / 
                    SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy),1), 
                '-'
            ),              --业务毛利同步上周同期
            ROUND(SUM(cur.gmv) * 0.05,1) AS platform_money
        FROM (    
            SELECT 
                dt,
                city_id,
                serv_type,
                finish_order_cnt,
                gmv,
                b_subsidy,
                c_subsidy, 
                b_deduction, 
                amount_service 
            FROM 
                oride_bi.oride_night_city_metrics_report
            WHERE dt = '{day}'
            ) AS cur
        LEFT JOIN (
            SELECT 
                city_id,
                serv_type,
                finish_order_cnt,
                gmv,
                b_subsidy,
                c_subsidy, 
                b_deduction, 
                amount_service 
            FROM 
                oride_bi.oride_night_city_metrics_report
            WHERE dt = '{bf7day}'
            ) yesterday 
        ON cur.city_id = yesterday.city_id AND 
            cur.serv_type = yesterday.serv_type 
        GROUP BY cur.dt 
        
        UNION ALL 
        
        SELECT 
            cur.dt,
            c.name,
            CASE 
                WHEN cur.serv_type=1 THEN 'Green'  
                WHEN cur.serv_type=2 THEN 'Street' 
                WHEN cur.serv_type=3 THEN 'OTrike' 
                ELSE 'Other' 
                END, 
            SUM(cur.finish_order_cnt),      --完单数
            IF(SUM(yesterday.finish_order_cnt)>0, 
                ROUND(SUM(cur.finish_order_cnt)*100/SUM(yesterday.finish_order_cnt),1), 
                '-' 
            ),                          --完单数同比上周同期
            SUM(cur.gmv),   --gmv
            IF(SUM(yesterday.gmv)>0, 
                ROUND(SUM(cur.gmv)*100/SUM(yesterday.gmv),1), 
                '-'
            ),                          --GMV同比上周同期
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.gmv)/SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS price_avg,             --单均应付
            IF(IF(SUM(yesterday.finish_order_cnt)>0, SUM(yesterday.gmv)/SUM(yesterday.finish_order_cnt), 0)>0,
                ROUND(IF(SUM(cur.finish_order_cnt)>0, SUM(cur.gmv)/SUM(cur.finish_order_cnt), 0) * 100 /
                    (SUM(yesterday.gmv)/SUM(yesterday.finish_order_cnt)), 1), 
                '-'
            ) AS price_avg_compare,     --单均应付对比上周同期
            SUM(cur.b_subsidy + cur.c_subsidy),         --总补贴
            IF(SUM(cur.gmv)>0, 
                ROUND((SUM(cur.b_subsidy + cur.c_subsidy)) * 100 / SUM(cur.gmv),1), 
                '-' 
            ) AS subsidy_rate,          --总补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND((SUM(cur.b_subsidy + cur.c_subsidy))  / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS subsidy_avg,           --单均补贴
            IF(IF(SUM(yesterday.finish_order_cnt)>0, (SUM(yesterday.b_subsidy + yesterday.c_subsidy))  / SUM(yesterday.finish_order_cnt), 0)>0,
                ROUND(IF(SUM(cur.finish_order_cnt)>0, (SUM(cur.b_subsidy + cur.c_subsidy))  / SUM(cur.finish_order_cnt), 0) * 100 /
                    ((SUM(yesterday.b_subsidy + yesterday.c_subsidy))  / SUM(yesterday.finish_order_cnt)),1), 
                '-'
            ) AS subsidy_avg_compare,   --单均补贴对比上周同期
            SUM(cur.b_subsidy) AS b_subsidy, --b端补贴
            IF(SUM(cur.gmv)>0, 
                ROUND(SUM(cur.b_subsidy) * 100/ SUM(cur.gmv),1), 
                '-' 
            ) AS b_subsidy_rate,        --b端补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.b_subsidy) / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS b_subsidy_avg,         --b端单均补贴
            SUM(cur.c_subsidy) AS c_subsidy, --c端补贴
            IF(SUM(cur.gmv)>0, 
                ROUND(SUM(cur.c_subsidy) * 100 / SUM(cur.gmv),1), 
                '-' 
            ) AS c_subsidy_rate ,       --c端补贴率
            IF(SUM(cur.finish_order_cnt)>0, 
                ROUND(SUM(cur.c_subsidy) / SUM(cur.finish_order_cnt),1), 
                '-' 
            ) AS c_subsidy_avg,         --c端单均补贴
            SUM(cur.amount_service + cur.b_deduction - cur.b_subsidy - cur.c_subsidy),      --业务毛利  
            IF(SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy)<>0 AND 
                SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy) IS NOT NULL, 
                ROUND(SUM(cur.amount_service+cur.b_deduction-cur.b_subsidy-cur.c_subsidy) / 
                    SUM(yesterday.amount_service+yesterday.b_deduction-yesterday.b_subsidy-yesterday.c_subsidy),1), 
                '-'
            ),              --业务毛利同步上周同期
            ROUND(SUM(cur.gmv) * 0.05,1) AS platform_money 
        FROM 
            (    
            SELECT 
                dt,
                city_id,
                serv_type,
                finish_order_cnt,
                gmv,
                b_subsidy,
                c_subsidy, 
                b_deduction, 
                amount_service 
            FROM 
                oride_bi.oride_night_city_metrics_report
            WHERE dt = '{day}'
            ) AS cur
        JOIN (
            SELECT 
                id,
                name
            FROM 
                oride_dw_ods.ods_sqoop_base_data_city_conf_df 
            WHERE dt = '{db1}'
                AND id != 999001
            ) AS c ON c.id = cur.city_id
        LEFT JOIN (
            SELECT 
                city_id,
                serv_type,
                finish_order_cnt,
                gmv,
                b_subsidy,
                c_subsidy, 
                b_deduction, 
                amount_service 
            FROM 
                oride_bi.oride_night_city_metrics_report
            WHERE dt = '{bf7day}'
            ) AS yesterday 
        ON cur.city_id = yesterday.city_id AND 
            cur.serv_type = yesterday.serv_type 
        GROUP BY cur.dt, c.name, cur.serv_type 
    
    '''.format(
        bf7day=yesterday,
        db1=db1,
        day=day
    )

    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()

    row_html = ''
    if len(res) > 0:
        tr_fmt = '''
               <tr>{row}</tr>
            '''
        row_fmt = '''
                <!--{}-->
                <td>{}</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}%</td>
                <td>{}</td>
                <td>{}</td>
                <td>{}%</td>
                <!--<td>{}</td>-->
                
            '''

        for data in res:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
        return row_html


conn_conf_dict = {}
# host, port, schema, login, password = get_db_conf('sqoop_db')
for oride_table in table_list:
    conn_id = oride_table.get('conn')
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = get_db_conf(conn_id)

    host, port, schema, login, password = conn_conf_dict[conn_id]

    """
    sqoop导入mysql数据到hive
    """
    import_table = BashOperator(
        task_id='import_table_{}'.format(oride_table.get('db') + "_" + oride_table.get('table')),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password \'{password}\' \
            --table {table} \
            --target-dir {table_path}/country_code=nal/dt={{{{ tomorrow_ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=host,
            port=port,
            schema=schema,
            username=login,
            password=password,
            table=oride_table.get('table'),
            table_path=s3path.format(bs=oride_table.get('table'))
        ),
        dag=dag,
    )

    """
    创建hive数据表任务
    """
    create_table = PythonOperator(
        task_id='create_table_{}'.format(hive_table.format(bs=oride_table.get('table'))),
        python_callable=create_hive_external_table,
        provide_context=True,
        op_kwargs={
            'db': oride_table.get('db'),
            'table': oride_table.get('table'),
            'conn': oride_table.get('conn')
        },
        dag=dag
    )

    """
    添加hive数据表分区
    """
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(hive_table.format(bs=oride_table.get('table'))),
        hql='''
            ALTER TABLE {hive_db}.{hive_table} ADD IF NOT EXISTS PARTITION (country_code='nal', dt='{{{{ tomorrow_ds }}}}')
        '''.format(
            hive_db=hive_db,
            hive_table=hive_table.format(bs=oride_table.get('table'))
        ),
        schema=hive_db,
        dag=dag
    )

    import_table >> create_table >> add_partitions >> insert_city_metrics >> send_report
