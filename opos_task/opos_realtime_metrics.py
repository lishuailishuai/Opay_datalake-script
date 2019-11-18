import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from utils.util import on_success_callback
from airflow.sensors.sql_sensor import SqlSensor

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 11, 9),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': on_success_callback,
}

schedule_interval = "*/10 * * * *"

dag = airflow.DAG(
    'opos_realtime_metrics',
    schedule_interval=schedule_interval,
    concurrency=15,
    max_active_runs=1,
    default_args=args)

pssp_mysql_hook = MySqlHook("opos_ptsp_db")
ptsp_mysql_conn = pssp_mysql_hook.get_conn()
ptsp_mysql_cursor = ptsp_mysql_conn.cursor()

opos_mysql_hook = MySqlHook("mysql_dw")
opos_mysql_conn = opos_mysql_hook.get_conn()
opos_mysql_cursor = opos_mysql_conn.cursor()

insert_sql_template = """
      insert into opos_dw.opos_order (
        order_id , 
        device_no , 
        cfrom  , 
        receipt_id  , 
        sender_id  , 
        bill_create_ip  , 
        org_pp_trade_no  , 
        pp_trade_no  , 
        payment_id , 
        org_payment_amount , 
        pay_type  , 
        pay_amount  , 
        merchant_activity_id  , 
        merchant_activity_type  , 
        merchant_activity_title  , 
        threshold_amount  , 
        threshold_orders , 
        activity_type  , 
        activity_title  , 
        activity_id  , 
        discount_ids , 
        discount_amount  , 
        return_amount  , 
        user_subsidy  , 
        order_type  , 
        pay_cur  , 
        trade_type  , 
        trade_status  , 
        merchant_subsidy_status  , 
        user_subsidy_status  , 
        first_order  , 
        resp_code  , 
        resp_message  , 
        query_resp_code  , 
        query_resp_message  , 
        auth_code  , 
        trade_version  , 
        reversal_type  , 
        refund_code  , 
        repaired  , 
        create_time  , 
        modify_time  , 
        resp_time  , 
        goods_desc  , 
        remark  , 
        sn  , 
        pos_user_data  , 
        user_risk_status  , 
        user_risk_code  , 
        user_risk_remark  , 
        merchant_risk_status  , 
        merchant_risk_code  , 
        merchant_risk_remark
    )
    values(
        '{order_id}' , 
        '{device_no}' , 
        '{cfrom}' , 
        '{receipt_id}'  ,
        '{sender_id}'  ,
        '{bill_create_ip}'  ,
        '{org_pp_trade_no}'  ,
        '{pp_trade_no}'  ,
        '{payment_id}' , 
        {org_payment_amount} , 
        '{pay_type}'  ,
        {pay_amount}  ,
        '{merchant_activity_id}'  ,
        '{merchant_activity_type}'  ,
        '{merchant_activity_title}'  ,
        {threshold_amount}  ,
        {threshold_orders} , 
        '{activity_type}'  ,
        '{activity_title}'  ,
        '{activity_id}'  ,
        '{discount_ids}' , 
        {discount_amount} ,
        {return_amount}  ,
        {user_subsidy}  ,
        '{order_type}'  ,
        '{pay_cur}'  ,
        '{trade_type}'  ,
        '{trade_status}'  ,
        '{merchant_subsidy_status}'  ,
        '{user_subsidy_status}'  ,
        '{first_order}'  ,
        '{resp_code}'  ,
        '{resp_message}'  ,
        '{query_resp_code}'  ,
        '{query_resp_message}'  ,
        '{auth_code}'  ,
        '{trade_version}'  ,
        '{reversal_type}'  ,
        '{refund_code}'  ,
        '{repaired}'  ,
        '{create_time}'  ,
        '{modify_time}'  ,
        '{resp_time}'  ,
        '{goods_desc}'  ,
        '{remark}'  ,
        '{sn}'  ,
        '{pos_user_data}'  ,
        '{user_risk_status}'  ,
        '{user_risk_code}'  ,
        '{user_risk_remark}'  ,
        '{merchant_risk_status}'  ,
        '{merchant_risk_code}'  ,
        '{merchant_risk_remark}'
    ) 
    ON DUPLICATE KEY
    UPDATE
     order_id=VALUES(order_id), 
        device_no=VALUES(device_no) , 
        cfrom=VALUES(cfrom) , 
        receipt_id=VALUES(receipt_id)  , 
        sender_id=VALUES(sender_id)  , 
        bill_create_ip=VALUES(bill_create_ip)  , 
        org_pp_trade_no=VALUES(org_pp_trade_no)  , 
        pp_trade_no=VALUES(pp_trade_no)  , 
        payment_id=VALUES(payment_id) , 
        org_payment_amount=VALUES(org_payment_amount) , 
        pay_type=VALUES(pay_type)  , 
        pay_amount=VALUES(pay_amount)  , 
        merchant_activity_id=VALUES(merchant_activity_id)  , 
        merchant_activity_type=VALUES(merchant_activity_type)  , 
        merchant_activity_title=VALUES(merchant_activity_title)  , 
        threshold_amount=VALUES(threshold_amount)  , 
        threshold_orders=VALUES(threshold_orders) , 
        activity_type=VALUES(activity_type)  , 
        activity_title=VALUES(activity_title)  , 
        activity_id=VALUES(activity_id)  , 
        discount_ids=VALUES(discount_ids) , 
        discount_amount=VALUES(discount_amount)  , 
        return_amount=VALUES(return_amount)  , 
        user_subsidy=VALUES(user_subsidy)  , 
        order_type=VALUES(order_type)  , 
        pay_cur=VALUES(pay_cur)  , 
        trade_type=VALUES(trade_type)  , 
        trade_status=VALUES(trade_status)  , 
        merchant_subsidy_status=VALUES(merchant_subsidy_status)  , 
        user_subsidy_status=VALUES(user_subsidy_status)  , 
        first_order=VALUES(first_order)  , 
        resp_code=VALUES(resp_code)  , 
        resp_message=VALUES(resp_message)  , 
        query_resp_code=VALUES(query_resp_code)  , 
        query_resp_message=VALUES(query_resp_message)  , 
        auth_code=VALUES(auth_code)  , 
        trade_version=VALUES(trade_version)  , 
        reversal_type=VALUES(reversal_type)  , 
        refund_code=VALUES(refund_code)  , 
        repaired=VALUES(repaired)  , 
        create_time=VALUES(create_time)  , 
        modify_time=VALUES(modify_time)  , 
        resp_time=VALUES(resp_time)  , 
        goods_desc=VALUES(goods_desc)  , 
        remark=VALUES(remark)  , 
        sn=VALUES(sn)  , 
        pos_user_data=VALUES(pos_user_data)  , 
        user_risk_status=VALUES(user_risk_status)  , 
        user_risk_code=VALUES(user_risk_code)  , 
        user_risk_remark=VALUES(user_risk_remark)  , 
        merchant_risk_status=VALUES(merchant_risk_status)  , 
        merchant_risk_code=VALUES(merchant_risk_code)  , 
        merchant_risk_remark=VALUES(merchant_risk_remark)
"""

query_sql_template = '''
        select 
        order_id , 
        device_no , 
        cfrom  , 
        receipt_id  , 
        sender_id  , 
        bill_create_ip  , 
        org_pp_trade_no  , 
        pp_trade_no  , 
        payment_id , 
        org_payment_amount , 
        pay_type  , 
        pay_amount  , 
        merchant_activity_id  , 
        merchant_activity_type  , 
        merchant_activity_title  , 
        threshold_amount  , 
        threshold_orders , 
        activity_type  , 
        activity_title  , 
        activity_id  , 
        discount_ids , 
        discount_amount  , 
        return_amount  , 
        user_subsidy  , 
        order_type  , 
        pay_cur  , 
        trade_type  , 
        trade_status  , 
        merchant_subsidy_status  , 
        user_subsidy_status  , 
        first_order  , 
        resp_code  , 
        resp_message  , 
        query_resp_code  , 
        query_resp_message  , 
        auth_code  , 
        trade_version  , 
        reversal_type  , 
        refund_code  , 
        repaired  , 
        create_time  , 
        modify_time  , 
        resp_time  , 
        goods_desc  , 
        remark  , 
        sn  , 
        pos_user_data  , 
        user_risk_status  , 
        user_risk_code  , 
        user_risk_remark  , 
        merchant_risk_status  , 
        merchant_risk_code  , 
        merchant_risk_remark  
        from 
        opos_payment_order_{year}_{week}
        where 
        (DATE_FORMAT(create_time,"%Y-%m-%d") = '{ds}' or DATE_FORMAT(modify_time,"%Y-%m-%d")='{ds}')
        or 
        (DATE_FORMAT(create_time,"%Y-%m-%d") = '{yesterday}' or DATE_FORMAT(modify_time,"%Y-%m-%d")='{yesterday}')
    '''


def insert_order_data(ds, **kwargs):
    year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
    week = datetime.strptime(ds, '%Y-%m-%d').strftime('%W')
    query_sql = query_sql_template.format(year=year, week=(int(week) + 1), ds=ds,
                                          yesterday=airflow.macros.ds_add(ds, -1))

    logging.info(query_sql)

    ptsp_mysql_cursor.execute(query_sql)
    results = ptsp_mysql_cursor.fetchall()

    logging.info(" record num : {num}".format(num=len(results)))

    for data in results:
        [order_id,
         device_no,
         cfrom,
         receipt_id,
         sender_id,
         bill_create_ip,
         org_pp_trade_no,
         pp_trade_no,
         payment_id,
         org_payment_amount,
         pay_type,
         pay_amount,
         merchant_activity_id,
         merchant_activity_type,
         merchant_activity_title,
         threshold_amount,
         threshold_orders,
         activity_type,
         activity_title,
         activity_id,
         discount_ids,
         discount_amount,
         return_amount,
         user_subsidy,
         order_type,
         pay_cur,
         trade_type,
         trade_status,
         merchant_subsidy_status,
         user_subsidy_status,
         first_order,
         resp_code,
         resp_message,
         query_resp_code,
         query_resp_message,
         auth_code,
         trade_version,
         reversal_type,
         refund_code,
         repaired,
         create_time,
         modify_time,
         resp_time,
         goods_desc,
         remark,
         sn,
         pos_user_data,
         user_risk_status,
         user_risk_code,
         user_risk_remark,
         merchant_risk_status,
         merchant_risk_code,
         merchant_risk_remark] = list(data)

        insert_sql = insert_sql_template.format(
            order_id=order_id,
            device_no=device_no,
            cfrom=cfrom,
            receipt_id=receipt_id,
            sender_id=sender_id,
            bill_create_ip=bill_create_ip,
            org_pp_trade_no=org_pp_trade_no,
            pp_trade_no=pp_trade_no,
            payment_id=payment_id,
            org_payment_amount=org_payment_amount,
            pay_type=pay_type,
            pay_amount=pay_amount,
            merchant_activity_id=merchant_activity_id,
            merchant_activity_type=merchant_activity_type,
            merchant_activity_title=merchant_activity_title,
            threshold_amount=threshold_amount,
            threshold_orders=threshold_orders,
            activity_type=activity_type,
            activity_title=activity_title,
            activity_id=activity_id,
            discount_ids=discount_ids,
            discount_amount=discount_amount,
            return_amount=return_amount,
            user_subsidy=user_subsidy,
            order_type=order_type,
            pay_cur=pay_cur,
            trade_type=trade_type,
            trade_status=trade_status,
            merchant_subsidy_status=merchant_subsidy_status,
            user_subsidy_status=user_subsidy_status,
            first_order=first_order,
            resp_code=resp_code,
            resp_message=resp_message,
            query_resp_code=query_resp_code,
            query_resp_message=query_resp_message,
            auth_code=auth_code,
            trade_version=trade_version,
            reversal_type=reversal_type,
            refund_code=refund_code,
            repaired=repaired,
            create_time=create_time,
            modify_time=modify_time,
            resp_time=resp_time,
            goods_desc=goods_desc,
            remark=remark,
            sn=sn,
            pos_user_data=pos_user_data,
            user_risk_status=user_risk_status,
            user_risk_code=user_risk_code,
            user_risk_remark=user_risk_remark,
            merchant_risk_status=merchant_risk_status,
            merchant_risk_code=merchant_risk_code,
            merchant_risk_remark=merchant_risk_remark)

        # logging.info(insert_sql)
        opos_mysql_cursor.execute(insert_sql)
        opos_mysql_conn.commit()


insert_order_data = PythonOperator(
    task_id='insert_order_data',
    python_callable=insert_order_data,
    provide_context=True,
    dag=dag
)

create_order_metrics_data = BashOperator(
    task_id='create_order_metrics_data',
    bash_command="""
        mysql -udml_insert -p6VaEyu -h10.52.149.112 opos_dw  -e "

            insert into opos_dw.opos_metrcis_realtime (
            dt,
            city_id,
            bd_id,
            pos_complete_order_cnt,
            qr_complete_order_cnt,
            gmv,
            have_order_merchant_cnt,
            active_user_cnt,
            pos_active_user_cnt,
            qr_active_user_cnt,
            new_user_cnt,
            pos_new_user_cnt,
            qr_new_user_cnt
            ) 

            select
            t.dt,
            t.city_id,
            t.bd_id,
            t.pos_complete_order_cnt,
            t.qr_complete_order_cnt,
            t.gmv,
            t.have_order_merchant_cnt,
            t.active_user_cnt,
            t.pos_active_user_cnt,
            t.qr_active_user_cnt,
            t.new_user_cnt,
            t.pos_new_user_cnt,
            t.qr_new_user_cnt

            from
            (
              select
              t.dt as dt,
              t.city_id as city_id,
              t.bd_id as bd_id,
              ifnull(count(if(t.order_type = 'pos' and t.trade_status = 'SUCCESS',t.order_id,null)),0) as pos_complete_order_cnt,
              ifnull(count(if(t.order_type = 'qrcode' and t.trade_status = 'SUCCESS',t.order_id,null)),0) as qr_complete_order_cnt,
              ifnull(sum(if(t.trade_status = 'SUCCESS',t.org_payment_amount,null)),0) as gmv,
              ifnull(count(distinct if(t.trade_status = 'SUCCESS',t.receipt_id,null)),0) as have_order_merchant_cnt,
              ifnull(count(distinct if(t.trade_status = 'SUCCESS',t.sender_id,null)),0) as active_user_cnt,
              ifnull(count(distinct if(t.order_type = 'pos' and t.trade_status = 'SUCCESS',t.sender_id,null)),0) as pos_active_user_cnt,
              ifnull(count(distinct if(t.order_type = 'qrcode' and t.trade_status = 'SUCCESS',t.sender_id,null)),0) as qr_active_user_cnt,
              
              ifnull(count(distinct if(t.trade_status = 'SUCCESS' and t.first_order = '1',t.sender_id,null)),0) as new_user_cnt,
              ifnull(count(distinct if(t.order_type = 'pos' and t.trade_status = 'SUCCESS' and t.first_order = '1',t.sender_id,null)),0) as pos_new_user_cnt,
              ifnull(count(distinct if(t.order_type = 'qrcode' and t.trade_status = 'SUCCESS' and t.first_order = '1',t.sender_id,null)),0) as qr_new_user_cnt


              from
              (   select
                  o.dt,
                  o.order_id,
                  o.receipt_id,
                  o.sender_id,
                  o.order_type,
                  o.trade_status,
                  o.org_payment_amount,
                  s.bd_id,
                  s.city_id,
                  o.first_order
                  from
                  bd_shop s
                  join
                  (
                      select
                      DATE_FORMAT(create_time,'%Y-%m-%d') as dt,
                      order_id,
                      receipt_id,
                      sender_id,
                      order_type,
                      trade_status,
                      ifnull(org_payment_amount,0) as org_payment_amount,
                      first_order

                      from
                      opos_order
                      where 
                      (DATE_FORMAT(create_time,'%Y-%m-%d') = '{{ ds }}' or 
                      DATE_FORMAT(create_time,'%Y-%m-%d') = '{{ macros.ds_add(ds, -1) }}'
                      )
                  ) o
                  on o.receipt_id = concat(s.opay_id,'')
               ) t
              group by t.dt,t.bd_id,t.city_id
            ) t
            ON DUPLICATE KEY
            UPDATE
            dt=VALUES(dt),
            city_id=VALUES(city_id),
            bd_id=VALUES(bd_id),
            pos_complete_order_cnt=VALUES(pos_complete_order_cnt),
            qr_complete_order_cnt=VALUES(qr_complete_order_cnt),
            gmv=VALUES(gmv),
            have_order_merchant_cnt=VALUES(have_order_merchant_cnt),
            active_user_cnt=VALUES(active_user_cnt),
            pos_active_user_cnt=VALUES(pos_active_user_cnt),
            qr_active_user_cnt=VALUES(qr_active_user_cnt),
            new_user_cnt=VALUES(new_user_cnt),
            pos_new_user_cnt=VALUES(pos_new_user_cnt),
            qr_new_user_cnt=VALUES(qr_new_user_cnt)

            ;

        "
    """,
    dag=dag,
)

# create_merchant_metrics_data = BashOperator(
#     task_id='create_merchant_metrics_data',
#     bash_command="""
#         mysql -udml_insert -p6VaEyu -h10.52.149.112 opos_dw  -e "
#
#             insert into opos_dw.opos_shop_metrcis_realtime (dt,city_id,bd_id,new_shop_cnt)
#
#             select
#             t.dt,
#             t.city_id,
#             t.bd_id,
#             t.new_shop_cnt
#
#             from
#             (
#                 select
#                 DATE_FORMAT(created_at,'%Y-%m-%d') as dt,
#                 city_id,
#                 bd_id,
#                 count(id) as new_shop_cnt
#
#                 from
#                 bd_shop
#                 where
#                 DATE_FORMAT(created_at,'%Y-%m-%d') = '{{ ds }}' or
#                 group by
#                 city_id,
#                 bd_id
#             ) t
#
#
#             ON DUPLICATE KEY
#             UPDATE
#             dt=VALUES(dt),
#             city_id=VALUES(city_id),
#             bd_id=VALUES(bd_id),
#             new_shop_cnt=VALUES(new_shop_cnt)
#
#             ;
#
#         "
#     """,
#     dag=dag,
# )
#
delete_old_order = BashOperator(
    task_id='delete_old_order',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_order where DATE_FORMAT(create_time,'%Y-%m-%d') < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

insert_order_data >> create_order_metrics_data >> delete_old_order
# insert_order_data >> create_merchant_metrics_data >> delete_old_order
