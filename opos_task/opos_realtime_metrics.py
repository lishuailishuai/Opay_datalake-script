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
    # 'email': ['bigdata_dw@opay-inc.com'],
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

opos_cashback_mysql_hook = MySqlHook("opos_cashback")
opos_cashback_mysql_conn = opos_cashback_mysql_hook.get_conn()
opos_cashback_mysql_cursor = opos_cashback_mysql_conn.cursor()

insert_order_bonus_extend_sql_template = """
    insert into opos_dw.opos_order_bonus_extend (
        id,
        qr_code,
        lng,
        lat,
        bd_id,
        bdm_id,
        rm_id,
        cm_id,
        hcm_id,
        bonus_id,
        scan_user,
        amount,
        risk_code,
        code,
        message,
        time 
    )
    values(
        {id},
        '{qr_code}',
        {lng},
        {lat},
        {bd_id},
        {bdm_id},
        {rm_id},
        {cm_id},
        {hcm_id},
        {bonus_id},
        '{scan_user}',
        {amount},
        '{risk_code}',
        {code},
        '{message}',
        '{time}'
    )
    ON DUPLICATE KEY
    UPDATE
    id=VALUES(id), 
    qr_code=VALUES(qr_code), 
    lng=VALUES(lng), 
    lat=VALUES(lat), 
    bd_id=VALUES(bd_id), 
    bdm_id=VALUES(bdm_id), 
    rm_id=VALUES(rm_id), 
    cm_id=VALUES(cm_id), 
    hcm_id=VALUES(hcm_id), 
    bonus_id=VALUES(bonus_id), 
    scan_user=VALUES(scan_user), 
    amount=VALUES(amount), 
    risk_code=VALUES(risk_code), 
    code=VALUES(code), 
    message=VALUES(message), 
    time=VALUES(time)

"""

query_order_bonus_extend_sql_template = """
    select 
        id,
        qr_code,
        lng,
        lat,
        bd_id,
        bdm_id,
        rm_id,
        cm_id,
        hcm_id,
        bonus_id,
        scan_user,
        amount,
        risk_code,
        code,
        message,
        time
    from 
        opos_scan_history
        where 
        (DATE_FORMAT(time,"%Y-%m-%d") = '{ds}')

"""

insert_order_bonus_sql_template = """
    insert into opos_dw.opos_order_bonus (
        id ,
        activity_id ,
        bd_id ,
        city_id ,
        device_id ,
        opay_account ,
        provider_account ,
        receiver_account ,
        amount ,
        use_amount ,
        bonus_rate ,
        bonus_amount ,
        status ,
        settle_status ,
        settle_type,
        reason ,
        risk_id ,
        settle_time,
        expire_time,
        use_time,
        use_date ,
        create_time ,
        update_time 
    )
    values(
        {id} ,
        {activity_id} ,
        {bd_id} ,
        {city_id} ,
        '{device_id}' ,
        '{opay_account}' ,
        '{provider_account}' ,
        '{receiver_account}' ,
        {amount} ,
        {use_amount} ,
        {bonus_rate} ,
        {bonus_amount} ,
        {status} ,
        {settle_status} ,
        {settle_type},
        '{reason}' ,
        '{risk_id}' ,
        {settle_time_str}
        {expire_time_str}
        {use_time_str}
        '{use_date}' ,
        '{create_time}' ,
        '{update_time}' 
    )
    ON DUPLICATE KEY
    UPDATE
    id=VALUES(id), 
    activity_id=VALUES(activity_id), 
    bd_id=VALUES(bd_id), 
    city_id=VALUES(city_id), 
    device_id=VALUES(device_id), 
    opay_account=VALUES(opay_account), 
    provider_account=VALUES(provider_account), 
    receiver_account=VALUES(receiver_account), 
    amount=VALUES(amount), 
    use_amount=VALUES(use_amount), 
    bonus_rate=VALUES(bonus_rate), 
    bonus_amount=VALUES(bonus_amount), 
    status=VALUES(status), 
    settle_status=VALUES(settle_status), 
    settle_type=VALUES(settle_type), 
    reason=VALUES(reason), 
    risk_id=VALUES(risk_id), 
    settle_time=VALUES(settle_time), 
    expire_time=VALUES(expire_time), 
    use_time=VALUES(use_time), 
    use_date=VALUES(use_date), 
    create_time=VALUES(create_time), 
    update_time=VALUES(update_time)
    
    

"""

query_order_bonus_sql_template = """
    select 
        id ,
        activity_id ,
        bd_id ,
        city_id ,
        device_id ,
        opay_account ,
        provider_account ,
        receiver_account ,
        amount ,
        use_amount ,
        bonus_rate ,
        bonus_amount ,
        status ,
        settle_status ,
        settle_type,
        reason ,
        risk_id ,
        settle_time ,
        expire_time ,
        use_time ,
        use_date ,
        create_time ,
        update_time 
    from 
        opos_bonus_record
        where 
        (DATE_FORMAT(create_time,"%Y-%m-%d") = '{ds}' or DATE_FORMAT(update_time,"%Y-%m-%d")='{ds}')

"""

insert_order_sql_template = """
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

insert_order_extend_sql_template = """
    insert into opos_dw.opos_order_extend (
        order_id,
        opay_id,
        shop_phone,
        shop_id,
        city_id,
        category,
        bd_id,
        bdm_id,
        rm_id,
        cm_id,
        hcm_id,
        create_time
    ) values
    (
        '{order_id}',
        '{opay_id}',
        '{shop_phone}',
        '{shop_id}',
        '{city_id}',
        '{category}',
        {bd_id},
        {bdm_id},
        {rm_id},
        {cm_id},
        {hcm_id},
        '{create_time}'
    )
     ON DUPLICATE KEY
    UPDATE
     order_id=VALUES(order_id), 
     opay_id=VALUES(opay_id), 
     shop_phone=VALUES(shop_phone), 
     shop_id=VALUES(shop_id), 
     city_id=VALUES(city_id), 
     category=VALUES(category), 
     bd_id=VALUES(bd_id), 
     bdm_id=VALUES(bdm_id), 
     rm_id=VALUES(rm_id), 
     cm_id=VALUES(cm_id), 
     hcm_id=VALUES(hcm_id), 
     create_time=VALUES(create_time)

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
    '''

query_order_extend_sql_template = """
    select 
    order_id,
    opay_id,
    shop_phone,
    shop_id,
    city_id,
    category,
    bd_id,
    bdm_id,
    rm_id,
    cm_id,
    hcm_id,
    create_time
    from opos_payment_order_bd_{year}_{week}
    where (DATE_FORMAT(create_time,"%Y-%m-%d") = '{ds}' )
"""


def insert_order_data(ds, **kwargs):
    # year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
    # week = datetime.strptime(ds, '%Y-%m-%d').strftime('%W')

    t = datetime.strptime(ds, '%Y-%m-%d').isocalendar()
    year = t[0]
    week = t[1]

    insert_order(ds, airflow.macros.ds_add(ds, -1), week, year)
    insert_order_extend(ds, airflow.macros.ds_add(ds, -1), week, year)
    insert_order_bonus(ds, airflow.macros.ds_add(ds, -1), week, year)
    insert_order_bonus_extend(ds, airflow.macros.ds_add(ds, -1), week, year)


def insert_order_bonus_extend(ds, yesterday, week, year):
    query_sql = query_order_bonus_extend_sql_template.format(year=year, week=(int(week)), ds=ds,
                                                             yesterday=yesterday)

    logging.info(query_sql)
    opos_cashback_mysql_cursor.execute(query_sql)
    results = opos_cashback_mysql_cursor.fetchall()
    logging.info(" record num : {num}".format(num=len(results)))
    for data in results:

        original_columns = list(data)
        columns = list()

        for i in original_columns:
            if i is None:
                i = 'null'
            columns.append(i)

        [
            id,
            qr_code,
            lng,
            lat,
            bd_id,
            bdm_id,
            rm_id,
            cm_id,
            hcm_id,
            bonus_id,
            scan_user,
            amount,
            risk_code,
            code,
            message,
            time
        ] = columns

        insert_sql = insert_order_bonus_extend_sql_template.format(
            id=id,
            qr_code=qr_code,
            lng=lng,
            lat=lat,
            bd_id=bd_id,
            bdm_id=bdm_id,
            rm_id=rm_id,
            cm_id=cm_id,
            hcm_id=hcm_id,
            bonus_id=bonus_id,
            scan_user=scan_user,
            amount=amount,
            risk_code=risk_code,
            code=code,
            message='',
            time=time
        )

        insert_sql = insert_sql.replace("'null'", 'null')
        opos_mysql_cursor.execute(insert_sql)
        opos_mysql_conn.commit()


def insert_order_bonus(ds, yesterday, week, year):
    query_sql = query_order_bonus_sql_template.format(year=year, week=(int(week)), ds=ds,
                                                      yesterday=yesterday)

    logging.info(query_sql)
    opos_cashback_mysql_cursor.execute(query_sql)
    results = opos_cashback_mysql_cursor.fetchall()
    logging.info(" record num : {num}".format(num=len(results)))
    for data in results:

        original_columns = list(data)
        columns = list()

        for i in original_columns:
            if i is None:
                i = 'null'
            columns.append(i)

        [
            id,
            activity_id,
            bd_id,
            city_id,
            device_id,
            opay_account,
            provider_account,
            receiver_account,
            amount,
            use_amount,
            bonus_rate,
            bonus_amount,
            status,
            settle_status,
            settle_type,
            reason,
            risk_id,
            settle_time,
            expire_time,
            use_time,
            use_date,
            create_time,
            update_time
        ] = columns

        settle_time_str = ''
        if not settle_time:
            settle_time_str = 'null'
        else:
            settle_time_str = "'{settle_time}',".format(settle_time=settle_time)

        expire_time_str = ''
        if not expire_time:
            expire_time_str = 'null,'
        else:
            expire_time_str = "'{expire_time}',".format(expire_time=expire_time)

        use_time_str = ''
        if not use_time:
            use_time_str = 'null,'
        else:
            use_time_str = "'{use_time}',".format(use_time=use_time)

        insert_sql = insert_order_bonus_sql_template.format(
            id=id,
            activity_id=activity_id,
            bd_id=bd_id,
            city_id=city_id,
            device_id=device_id,
            opay_account=opay_account,
            provider_account=provider_account,
            receiver_account=receiver_account,
            amount=amount,
            use_amount=use_amount,
            bonus_rate=bonus_rate,
            bonus_amount=bonus_amount,
            status=status,
            settle_status=settle_status,
            settle_type=settle_type,
            reason=reason,
            risk_id=risk_id,
            settle_time_str=settle_time_str,
            expire_time_str=expire_time_str,
            use_time_str=use_time_str,
            use_date=use_date,
            create_time=create_time,
            update_time=update_time
        )

        insert_sql = insert_sql.replace("'null'", 'null')
        opos_mysql_cursor.execute(insert_sql)
        opos_mysql_conn.commit()


def insert_order_extend(ds, yesterday, week, year):
    query_sql = query_order_extend_sql_template.format(year=year, week=(int(week)), ds=ds,
                                                       yesterday=yesterday)

    logging.info(query_sql)
    ptsp_mysql_cursor.execute(query_sql)
    results = ptsp_mysql_cursor.fetchall()
    logging.info(" record num : {num}".format(num=len(results)))
    for data in results:
        [
            order_id,
            opay_id,
            shop_phone,
            shop_id,
            city_id,
            category,
            bd_id,
            bdm_id,
            rm_id,
            cm_id,
            hcm_id,
            create_time
        ] = list(data)

        insert_sql = insert_order_extend_sql_template.format(
            order_id=order_id,
            opay_id=opay_id,
            shop_phone=shop_phone,
            shop_id=shop_id,
            city_id=city_id,
            category=category,
            bd_id=bd_id,
            bdm_id=bdm_id,
            rm_id=rm_id,
            cm_id=cm_id,
            hcm_id=hcm_id,
            create_time=create_time
        )

        opos_mysql_cursor.execute(insert_sql)
        opos_mysql_conn.commit()


def insert_order(ds, yesterday, week, year):
    query_sql = query_sql_template.format(year=year, week=(int(week)), ds=ds,
                                          yesterday=yesterday)
    logging.info(query_sql)
    ptsp_mysql_cursor.execute(query_sql)
    results = ptsp_mysql_cursor.fetchall()
    logging.info(" record num : {num}".format(num=len(results)))

    for data in results:

        original_columns = list(data)
        columns = list()

        for i in original_columns:
            if i is None:
                i = ''
            columns.append(i)
        insert_sql = ''

        try:
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
             merchant_risk_remark] = list(columns)

            if org_payment_amount == '':
                org_payment_amount = 0
            if pay_amount == '':
                pay_amount = 0
            if threshold_amount == '':
                threshold_amount = 0
            if threshold_orders == '':
                threshold_orders = 0
            if discount_amount == '':
                discount_amount = 0
            if return_amount == '':
                return_amount = 0
            if user_subsidy == '':
                user_subsidy = 0

            insert_sql = insert_order_sql_template.format(
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

        except Exception as e:
            logging.info(insert_sql)
            logging.info(e)


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
            
            INSERT INTO opos_dw.opos_metrcis_realtime ( dt, city_id, bd_id,bdm_id,rm_id,cm_id,hcm_id, pos_complete_order_cnt, qr_complete_order_cnt, gmv, have_order_merchant_cnt, active_user_cnt, pos_active_user_cnt, qr_active_user_cnt, new_user_cnt, pos_new_user_cnt, qr_new_user_cnt, bonus_complete_order_cnt, bonus_gmv, bonus_new_user_cnt, bonus_active_user_cnt, bonus_actual_amount_sum, not_bonus_complete_order_cnt, not_bonus_gmv, not_bonus_actual_amount_sum, not_bonus_active_user_cnt )
            SELECT  t.dt 
                   ,t.city_id 
                   ,t.bd_id 
                   ,t.bdm_id 
                   ,t.rm_id 
                   ,t.cm_id 
                   ,t.hcm_id 
                   ,t.pos_complete_order_cnt 
                   ,t.qr_complete_order_cnt 
                   ,t.gmv 
                   ,t.have_order_merchant_cnt 
                   ,t.active_user_cnt 
                   ,t.pos_active_user_cnt 
                   ,t.qr_active_user_cnt 
                   ,t.new_user_cnt 
                   ,t.pos_new_user_cnt 
                   ,t.qr_new_user_cnt 
                   ,t.bonus_complete_order_cnt 
                   ,t.bonus_gmv 
                   ,t.bonus_new_user_cnt 
                   ,t.bonus_active_user_cnt 
                   ,t.bonus_actual_amount_sum 
                   ,t.not_bonus_complete_order_cnt 
                   ,t.not_bonus_gmv 
                   ,t.not_bonus_actual_amount_sum 
                   ,t.not_bonus_active_user_cnt
            FROM 
            (
            SELECT  t.dt                                                                                                                             AS dt 
                   ,t.city_id                                                                                                                        AS city_id 
                   ,t.bdm_id                                                                                                                         AS bdm_id 
                   ,t.rm_id                                                                                                                          AS rm_id 
                   ,t.cm_id                                                                                                                          AS cm_id 
                   ,t.hcm_id                                                                                                                         AS hcm_id 
                   ,t.bd_id                                                                                                                          AS bd_id 
                   ,ifnull(COUNT(if(t.order_type = 'pos' AND t.trade_status = 'SUCCESS',t.order_id,null)),0)                                         AS pos_complete_order_cnt 
                   ,ifnull(COUNT(if(t.order_type = 'qrcode' AND t.trade_status = 'SUCCESS',t.order_id,null)),0)                                      AS qr_complete_order_cnt 
                   ,ifnull(SUM(if(t.trade_status = 'SUCCESS',t.org_payment_amount,null)),0)                                                          AS gmv 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS',t.receipt_id,null)),0)                                                       AS have_order_merchant_cnt 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS',t.sender_id,null)),0)                                                        AS active_user_cnt 
                   ,ifnull(COUNT(distinct if(t.order_type = 'pos' AND t.trade_status = 'SUCCESS',t.sender_id,null)),0)                               AS pos_active_user_cnt 
                   ,ifnull(COUNT(distinct if(t.order_type = 'qrcode' AND t.trade_status = 'SUCCESS',t.sender_id,null)),0)                            AS qr_active_user_cnt 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS' AND t.first_order = '1',t.sender_id,null)),0)                                AS new_user_cnt 
                   ,ifnull(COUNT(distinct if(t.order_type = 'pos' AND t.trade_status = 'SUCCESS' AND t.first_order = '1',t.sender_id,null)),0)       AS pos_new_user_cnt 
                   ,ifnull(COUNT(distinct if(t.order_type = 'qrcode' AND t.trade_status = 'SUCCESS' AND t.first_order = '1',t.sender_id,null)),0)    AS qr_new_user_cnt 
                   ,ifnull(COUNT(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.order_id,null)),0)                                   AS bonus_complete_order_cnt 
                   ,ifnull(SUM(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.org_payment_amount,null)),0)                           AS bonus_gmv 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS' AND t.first_order = '1' AND length(t.discount_ids) > 0,t.sender_id,null)),0) AS bonus_new_user_cnt 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.sender_id,null)),0)                         AS bonus_active_user_cnt 
                   ,ifnull(SUM(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.payment_amount,null)),0)                               AS bonus_actual_amount_sum 
                   ,ifnull(COUNT(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) = 0,t.order_id,null)),0)                                   AS not_bonus_complete_order_cnt 
                   ,ifnull(SUM(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) = 0,t.org_payment_amount,null)),0)                           AS not_bonus_gmv 
                   ,ifnull(SUM(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) = 0,t.payment_amount,null)),0)                               AS not_bonus_actual_amount_sum 
                   ,ifnull(COUNT(distinct if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) = 0,t.sender_id,null)),0)                         AS not_bonus_active_user_cnt
            FROM 
            (
                SELECT  o.dt 
                       ,o.order_id 
                       ,o.receipt_id 
                       ,o.sender_id 
                       ,o.order_type 
                       ,o.trade_status 
                       ,o.org_payment_amount 
                       ,o.payment_amount 
                       ,s.bd_id 
                       ,s.city_id 
                       ,s.bdm_id 
                       ,s.rm_id 
                       ,s.cm_id 
                       ,s.hcm_id 
                       ,o.first_order 
                       ,o.discount_ids
                FROM 
                (
                    SELECT  order_id 
                           ,opay_id 
                           ,bd_id 
                           ,city_id 
                           ,bdm_id 
                           ,rm_id 
                           ,cm_id 
                           ,hcm_id
                    FROM opos_order_extend 
                ) s
                JOIN 
                (
                    SELECT  DATE_FORMAT(create_time,'%Y-%m-%d') AS dt 
                           ,order_id 
                           ,receipt_id 
                           ,sender_id 
                           ,order_type 
                           ,trade_status 
                           ,ifnull(org_payment_amount,0)        AS org_payment_amount 
                           ,ifnull(pay_amount,0)                AS payment_amount 
                           ,first_order 
                           ,discount_ids
                    FROM opos_order
                    WHERE (DATE_FORMAT(create_time,'%Y-%m-%d') = '{{ ds }}' )  
                ) o
                ON o.order_id = s.order_id 
            ) t
            GROUP BY  t.dt 
                     ,t.bd_id 
                     ,t.city_id 
                     ,t.bdm_id 
                     ,t.rm_id 
                     ,t.cm_id 
                     ,t.hcm_id 
            ) t
            ON DUPLICATE KEY UPDATE dt=VALUES(dt), city_id=VALUES(city_id), bd_id=VALUES(bd_id), bdm_id=VALUES(bdm_id), rm_id=VALUES(rm_id), cm_id=VALUES(cm_id), hcm_id=VALUES(hcm_id), pos_complete_order_cnt=VALUES(pos_complete_order_cnt), qr_complete_order_cnt=VALUES(qr_complete_order_cnt), gmv=VALUES(gmv), have_order_merchant_cnt=VALUES(have_order_merchant_cnt), active_user_cnt=VALUES(active_user_cnt), pos_active_user_cnt=VALUES(pos_active_user_cnt), qr_active_user_cnt=VALUES(qr_active_user_cnt), new_user_cnt=VALUES(new_user_cnt), pos_new_user_cnt=VALUES(pos_new_user_cnt), qr_new_user_cnt=VALUES(qr_new_user_cnt), bonus_complete_order_cnt=VALUES(bonus_complete_order_cnt), bonus_gmv=VALUES(bonus_gmv), bonus_new_user_cnt=VALUES(bonus_new_user_cnt), bonus_active_user_cnt=VALUES(bonus_active_user_cnt), bonus_actual_amount_sum=VALUES(bonus_actual_amount_sum), not_bonus_complete_order_cnt=VALUES(not_bonus_complete_order_cnt), not_bonus_gmv=VALUES(not_bonus_gmv), not_bonus_actual_amount_sum=VALUES(not_bonus_actual_amount_sum), not_bonus_active_user_cnt=VALUES(not_bonus_active_user_cnt) ; 

        "
    """,
    dag=dag,
)

create_bonus_metrics_data = BashOperator(
    task_id='create_bonus_metrics_data',
    bash_command="""
        mysql -udml_insert -p6VaEyu -h10.52.149.112 opos_dw  -e "

            INSERT INTO opos_dw.opos_bonus_metrics_realtime ( dt, city_id, bd_id,bdm_id,rm_id,cm_id,hcm_id, in_credit_amount_sum, not_in_credit_amount_sum, in_settlement_amount_sum, not_in_settlement_amount_sum, provider_cnt, bonus_order_cnt, bonus_user_cnt, main_scan_amount, bonus_used_amount_sum )
            SELECT  t.dt 
                   ,t.city_id 
                   ,t.bd_id 
                   ,t.bdm_id 
                   ,t.rm_id 
                   ,t.cm_id 
                   ,t.hcm_id 
                   ,t.in_credit_amount_sum 
                   ,t.not_in_credit_amount_sum 
                   ,t.in_settlement_amount_sum 
                   ,t.not_in_settlement_amount_sum 
                   ,t.provider_cnt 
                   ,t.bonus_order_cnt 
                   ,t.bonus_user_cnt 
                   ,t.main_scan_amount 
                   ,t.bonus_used_amount_sum
            FROM 
            (
            SELECT  '{{ ds }}'                                                     AS dt 
                   ,ifnull(t.city_id,-10000)                                       AS city_id 
                   ,ifnull(t.bd_id,-10000)                                         AS bd_id 
                   ,ifnull(e.bdm_id,-10000)                                        AS bdm_id 
                   ,ifnull(e.rm_id,-10000)                                         AS rm_id 
                   ,ifnull(e.cm_id,-10000)                                         AS cm_id 
                   ,ifnull(e.hcm_id,-10000)                                        AS hcm_id 
                   ,SUM(if(t.status = 1,t.bonus_amount,0))                         AS in_credit_amount_sum 
                   ,SUM(if(t.status = 0,t.bonus_amount,0))                         AS not_in_credit_amount_sum 
                   ,SUM(if(t.status = 1 AND t.settle_status = 1,t.bonus_amount,0)) AS in_settlement_amount_sum 
                   ,SUM(if(t.status = 1 AND t.settle_status = 0,t.bonus_amount,0)) AS not_in_settlement_amount_sum 
                   ,COUNT(distinct(t.provider_account))                            AS provider_cnt 
                   ,COUNT(1)                                                       AS bonus_order_cnt 
                   ,COUNT(distinct(t.opay_account))                                AS bonus_user_cnt 
                   ,SUM(t.amount)                                                  AS main_scan_amount 
                   ,SUM(t.use_amount)                                              AS bonus_used_amount_sum
            FROM 
            (
                SELECT  id 
                       ,city_id 
                       ,bd_id 
                       ,bonus_amount 
                       ,status 
                       ,settle_status 
                       ,provider_account 
                       ,opay_account 
                       ,amount 
                       ,use_amount
                FROM opos_dw.opos_order_bonus
                WHERE DATE_FORMAT(create_time,'%Y-%m-%d') = '{{ ds }}'  
            ) t
            LEFT JOIN 
            (
                SELECT  bonus_id 
                       ,bd_id 
                       ,bdm_id 
                       ,rm_id 
                       ,cm_id 
                       ,hcm_id
                FROM opos_dw.opos_order_bonus_extend
                WHERE DATE_FORMAT(time,'%Y-%m-%d') = '{{ ds }}'  
            ) e
            ON t.id = e.bonus_id
            GROUP BY  t.city_id 
                     ,t.bd_id 
                     ,e.bdm_id 
                     ,e.rm_id 
                     ,e.cm_id 
                     ,e.hcm_id with rollup 
            ) t
            ON DUPLICATE KEY UPDATE dt=VALUES(dt), city_id=VALUES(city_id), bd_id=VALUES(bd_id), bdm_id=VALUES(bdm_id), rm_id=VALUES(rm_id), cm_id=VALUES(cm_id), hcm_id=VALUES(hcm_id), in_credit_amount_sum=VALUES(in_credit_amount_sum), not_in_credit_amount_sum=VALUES(not_in_credit_amount_sum), in_settlement_amount_sum=VALUES(in_settlement_amount_sum), not_in_settlement_amount_sum=VALUES(not_in_settlement_amount_sum), provider_cnt=VALUES(provider_cnt), bonus_order_cnt=VALUES(bonus_order_cnt), bonus_user_cnt=VALUES(bonus_user_cnt), main_scan_amount=VALUES(main_scan_amount), bonus_used_amount_sum=VALUES(bonus_used_amount_sum) ;

        "
    """,
    dag=dag,
)

create_shop_metrics_data = BashOperator(
    task_id='create_shop_metrics_data',
    bash_command="""
        mysql -udml_insert -p6VaEyu -h10.52.149.112 opos_dw  -e "

            INSERT INTO opos_dw.opos_shop_metrics_realtime ( dt, city_id, bd_id,shop_id ,complete_order_cnt ,gmv ,active_user_cnt ,new_user_cnt ,bonus_complete_order_cnt ,bonus_gmv ,bonus_active_user_cnt ,bonus_new_user_cnt ,merchant_return_amount_sum ,all_order_cnt ,fail_order_cnt ,user_activity_order_cnt ,merchant_return_order_cnt )
            SELECT  t.dt 
                   ,t.city_id 
                   ,t.bd_id 
                   ,t.shop_id 
                   ,t.complete_order_cnt 
                   ,t.gmv 
                   ,t.active_user_cnt 
                   ,t.new_user_cnt 
                   ,t.bonus_complete_order_cnt 
                   ,t.bonus_gmv 
                   ,t.bonus_active_user_cnt 
                   ,t.bonus_new_user_cnt 
                   ,t.merchant_return_amount_sum 
                   ,t.all_order_cnt 
                   ,t.fail_order_cnt 
                   ,t.user_activity_order_cnt 
                   ,t.merchant_return_order_cnt
            FROM 
            (
            SELECT  t.dt 
                   ,t.city_id 
                   ,t.bd_id 
                   ,t.shop_id 
                   ,COUNT(if(t.trade_status = 'SUCCESS',t.order_id,null))                                                                  AS complete_order_cnt 
                   ,SUM(if(t.trade_status = 'SUCCESS',t.org_payment_amount,0))                                                             AS gmv 
                   ,COUNT(distinct if(t.trade_status = 'SUCCESS',sender_id,null))                                                          AS active_user_cnt 
                   ,COUNT(distinct if(t.trade_status = 'SUCCESS' AND t.first_order = '1',t.sender_id,null))                                AS new_user_cnt 
                   ,COUNT(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.order_id,null))                                   AS bonus_complete_order_cnt 
                   ,SUM(if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,t.org_payment_amount,0))                              AS bonus_gmv 
                   ,COUNT(distinct if(t.trade_status = 'SUCCESS' AND length(t.discount_ids) > 0,sender_id,null))                           AS bonus_active_user_cnt 
                   ,COUNT(distinct if(t.trade_status = 'SUCCESS' AND t.first_order = '1' AND length(t.discount_ids) > 0,t.sender_id,null)) AS bonus_new_user_cnt 
                   ,SUM(if(t.trade_status = 'SUCCESS',t.return_amount,0))                                                                  AS merchant_return_amount_sum 
                   ,COUNT(t.order_id)                                                                                                      AS all_order_cnt 
                   ,COUNT(if(t.trade_status = 'FAIL',t.order_id,null))                                                                     AS fail_order_cnt 
                   ,COUNT(if(t.org_payment_amount - t.payment_amount > 0,t.order_id,null))                                                 AS user_activity_order_cnt 
                   ,COUNT(if(t.trade_status = 'SUCCESS' AND t.return_amount > 0,t.order_id,null))                                          AS merchant_return_order_cnt
            FROM 
            (
                SELECT  o.dt 
                       ,o.order_id 
                       ,o.receipt_id 
                       ,o.sender_id 
                       ,o.order_type 
                       ,o.trade_status 
                       ,o.org_payment_amount 
                       ,o.payment_amount 
                       ,s.bd_id 
                       ,s.city_id 
                       ,s.shop_id 
                       ,o.first_order 
                       ,o.discount_ids 
                       ,o.return_amount
                FROM 
                (
                    SELECT  order_id 
                           ,shop_id 
                           ,opay_id 
                           ,bd_id 
                           ,city_id
                    FROM opos_order_extend 
                ) s
                JOIN 
                (
                    SELECT  DATE_FORMAT(create_time,'%Y-%m-%d') AS dt 
                           ,order_id 
                           ,receipt_id 
                           ,sender_id 
                           ,order_type 
                           ,trade_status 
                           ,ifnull(org_payment_amount,0)        AS org_payment_amount 
                           ,ifnull(pay_amount,0)                AS payment_amount 
                           ,first_order 
                           ,discount_ids 
                           ,return_amount
                    FROM opos_order
                    WHERE (DATE_FORMAT(create_time,'%Y-%m-%d') = '{{ ds }}' )  
                ) o
                ON o.order_id = s.order_id 
            ) t
            GROUP BY  t.dt 
                     ,t.bd_id 
                     ,t.city_id 
                     ,t.shop_id 
            ) t
            ON DUPLICATE KEY UPDATE dt=VALUES(dt), city_id=VALUES(city_id), bd_id=VALUES(bd_id), complete_order_cnt =VALUES(complete_order_cnt) , gmv =VALUES(gmv) , active_user_cnt =VALUES(active_user_cnt) ,new_user_cnt =VALUES(new_user_cnt) , bonus_complete_order_cnt =VALUES(bonus_complete_order_cnt) , bonus_gmv =VALUES(bonus_gmv) , bonus_active_user_cnt =VALUES(bonus_active_user_cnt) , bonus_new_user_cnt =VALUES(bonus_new_user_cnt) , merchant_return_amount_sum =VALUES(merchant_return_amount_sum) ,all_order_cnt =VALUES(all_order_cnt) ,fail_order_cnt =VALUES(fail_order_cnt) ,user_activity_order_cnt =VALUES(user_activity_order_cnt) ,merchant_return_order_cnt=VALUES(merchant_return_order_cnt) ;

        "
    """,
    dag=dag,
)

delete_old_order = BashOperator(
    task_id='delete_old_order',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_order where DATE_FORMAT(create_time,'%Y-%m-%d') < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

delete_old_order_extend = BashOperator(
    task_id='delete_old_order_extend',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_order_extend where DATE_FORMAT(create_time,'%Y-%m-%d') < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

delete_shop_metrics = BashOperator(
    task_id='delete_shop_metrics',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_shop_metrics_realtime where dt < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

delete_old_order_bonus = BashOperator(
    task_id='delete_old_order_bonus',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_dw.opos_order_bonus where DATE_FORMAT(create_time,'%Y-%m-%d') < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

delete_old_order_bonus_extend = BashOperator(
    task_id='delete_old_order_bonus_extend',
    bash_command="""
        mysql -uroot -p78c5f1142124334 -h10.52.149.112 opos_dw  -e "
            delete from opos_dw.opos_order_bonus_extend where DATE_FORMAT(time,'%Y-%m-%d') < '{{ macros.ds_add(ds, -1) }}';
        "
    """,
    dag=dag,
)

insert_order_data >> create_order_metrics_data >> create_shop_metrics_data >> delete_old_order >> delete_old_order_extend >> delete_shop_metrics
insert_order_data >> create_bonus_metrics_data >> delete_old_order_bonus >> delete_old_order_bonus_extend
