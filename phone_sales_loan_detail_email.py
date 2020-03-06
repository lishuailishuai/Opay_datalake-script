# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import xlwt
import logging
import codecs
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 5),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'phone_sales_loan_detail_email',
    schedule_interval="20 02 * * *",
    default_args=args)

table_names = ['ocredit_phones_dw_ods.ods_sqoop_base_t_order_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_order_down_payment_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_contract_pic_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_contract_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_order_audit_history_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_sys_user_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_sys_dept_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_repayment_plan_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_order_relate_user_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_financial_product_phone_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_merchant_store_opay_info_df',
               'ocredit_phones_dw_ods.ods_sqoop_base_t_repayment_detail_df'
               ]

'''
校验分区代码
'''

ods_sqoop_base_t_order_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_order_down_payment_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_down_payment_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order_down_payment",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_contract_pic_df_task = OssSensor(
    task_id='ods_sqoop_base_t_contract_pic_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_contract_pic",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_contract_df_task = OssSensor(
    task_id='ods_sqoop_base_t_contract_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_contract",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_order_audit_history_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_audit_history_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order_audit_history",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_sys_user_df_task = OssSensor(
    task_id='ods_sqoop_base_sys_user_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/admin_guns/sys_user",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_sys_dept_df_task = OssSensor(
    task_id='ods_sqoop_base_sys_dept_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/admin_guns/sys_dept",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_repayment_plan_df_task = OssSensor(
    task_id='ods_sqoop_base_t_repayment_plan_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_repayment_plan",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_order_relate_user_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_relate_user_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order_relate_user",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_financial_product_phone_df_task = OssSensor(
    task_id='ods_sqoop_base_t_financial_product_phone_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_financial_product_phone",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_merchant_store_opay_info_df_task = OssSensor(
    task_id='ods_sqoop_base_t_merchant_store_opay_info_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_merchant_store_opay_info",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_t_repayment_detail_df_task = OssSensor(
    task_id='ods_sqoop_base_t_repayment_detail_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_repayment_detail",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

def send_phone_sales_loan_detail_email(ds, **kwargs):
    cursor = get_hive_cursor()
    sales_statistics = '''
        select  a.order_id`订单号`,
                DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600),'yyyy-MM-dd')`进件日期`,
                DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600),'yyyy-MM-dd HH:mm:00')`进件时间`,
                cast(a.user_id as string) `SA_code/销售人员代码`,
                a.sale_name`SA_Name/销售人员姓名`, 
                k.simple_name `DMS名称`,
                k.full_name as regional_manger,
                case 
                when a.product_category=1 then '手机'
                when a.product_category=2 then '汽车'
                when a.product_category=3 then '摩托车'
                when a.product_category=4 then '家电'
                when a.product_category=5 then '电脑'
                else null
                end as `商品类型`,
                f.passtime `信审通过时间`,
                cast(a.merchant_id as string) `商户ID`,
                a.merchant_name `商户名称`,
                cast(a.store_id as string) `门店ID`,
                a.store_name `门店名称`,
                case 
                when a.order_status=10 then '等待初审'
                when a.order_status=11 then '初审通过'
                when a.order_status=12 then '初审失败'
                when a.order_status=13 then '初审驳回'
                when a.order_status=30 then '等待终审'
                when a.order_status=31 then '复审通过'
                when a.order_status=32 then '复审失败'
                when a.order_status>32 and a.order_status<99 then '复审通过'
                else '异常'
                end as `审核结果`,
                DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(b.pay_time)+3600),'yyyy-MM-dd HH:mm:00') `首付成功时间`,
                case when a.order_status in (80,81,82,83) then DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(c.create_time)+3600),'yyyy-MM-dd HH:mm:00') else null end `资料合同上传时间`,
                case when a.order_status in (80,81,82,83) then DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(d.last_audit_time)+3600),'yyyy-MM-dd HH:mm:00') else null end `合同审核通过时间`,
                round(a.down_payment/100,2)`首付金额`,
                round(a.loan_amount/100,2)`贷款总额`
                from  
                (select * 
                from ocredit_phones_dw_ods.ods_sqoop_base_t_order_df
                where dt='{pt}'
                and business_type=0
                and user_id not in 
                (
                '1209783514507214849', 
                '1209126038292123650',
                '1210903150317494274',
                '1214471918163460097',
                '1215642304343425026',
                '1226878328587288578')) a
                left join 
                (select * 
                from ocredit_phones_dw_ods.ods_sqoop_base_t_order_down_payment_df
                where dt='{pt}') b
                on a.order_id=b.order_id
                left join 
                (select order_id,max(create_time) create_time 
                from ocredit_phones_dw_ods.ods_sqoop_base_t_contract_pic_df
                where dt='{pt}'
                group by order_id) c
                on a.order_id=c.order_id
                left join 
                (select * 
                from ocredit_phones_dw_ods.ods_sqoop_base_t_contract_df
                where dt='{pt}') d
                on a.order_id=d.order_id
                left join 
                (select order_id,
                        DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(create_time)+3600),'yyyy-MM-dd HH:mm:00') passtime 
                from ocredit_phones_dw_ods.ods_sqoop_base_t_order_audit_history_df 
                where dt='{pt}' 
                and audit_type=2 
                and audit_status=1) f
                on a.order_id=f.order_id
                left join
                (select x1.user_id,x2.simple_name,x2.full_name 
                from 
                (select * 
                from ocredit_phones_dw_ods.ods_sqoop_base_sys_user_df
                where dt='{pt}') x1
                left join 
                (select * 
                from ocredit_phones_dw_ods.ods_sqoop_base_sys_dept_df
                where dt='{pt}') x2
                on x1.dept_id=x2.dept_id
                )  k
                on a.user_id=k.user_id
    '''.format(pt=ds, ld=airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))

    loan_amount_detail = '''
            select  a.order_id`订单号`,
                    DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600),'yyyy-MM-dd')`进件日期`,
                    DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600),'yyyy-MM-dd HH:mm:00')`进件时间`,
                    a.city`Region/地区`,
                    cast(a.merchant_id as string) `商户ID`,
                    a.merchant_name `商户名称`,
                    a.store_id`POS/门店代码`,
                    a.store_name`POS Name/门店名称 `,
                    cast(a.user_id as string) `SA_code/销售人员代码`,
                    k.simple_name `DMS名称`,
                    k.full_name as regional_manger,
                    a.sale_name`SA_Name/销售人员姓名`,
                    f.passtime `审核通过时间`,
                    c.full_name`客户姓名`,
                    x.user_phone `客户电话`,
                    case 
                    when x.user_city=24 then '拉各斯州'
                    when x.user_city=27 then '奥贡州'
                    when x.user_city=30 then '奥约州'
                    else '新城市'
                    end as `城市`,
                    case 
                    when a.product_category=1 then '手机'
                    when a.product_category=2 then '汽车'
                    when a.product_category=3 then '摩托车'
                    when a.product_category=4 then '家电'
                    when a.product_category=5 then '电脑'
                    else null
                    end as `商品类型`,
                    b.contract_id`合同编号`,
                    round(a.loan_price/100,2)`商品价格`,
                    round(a.loan_amount/100,2)`合同金额`,
                    round(a.loan_amount/100,2)`贷款总额`,
                    case 
                    when a.product_type=1 then '自营'
                    when a.product_type=2 then '合作'
                    else null
                    end as `业务类型`,
                    a.terms`分期数`,
                    g.loan_interest_rate+g.a_interest_rate`综合月费率`,
                    round(a.down_payment/100,2)`首付金额`,
                    a.payment_rate`首付比例`,
                    round(b.repayment_amount/100,2)`总还款金额`,
                    DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.loan_time)+3600),'yyyy-MM-dd')`放款日期`,
                    DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(a.loan_time)+3600),'yyyy-MM-dd HH:mm:00')`放款时间`,
                    case 
                    when b.repayment_type=0 then '等额本息'
                    when b.repayment_type=1 then '等额本金'
                    when b.repayment_type=2 then '等本等息'
                    when b.repayment_type=3 then '先息后本'
                    when b.repayment_type=4 then '随借随还'
                    else null
                    end as `还款方式`,
                    h.opay_agent_account`收款账户`,
                    DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(j.repayment_time)+3600),'yyyy-MM-dd HH:mm:00')`开始还款时间`,
                    round(j.month_amount/100,2)`每月应收本金`,
                    round(j.month_service_fee/100,2)`每月应收服务费`,
                    round(j.month_total_amount/100,2)`每月还款总额`
                    from 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_order_df
                    where dt='{pt}'
                    and order_status=81
                    and business_type=0
                    and user_id not in 
                    (
                    '1209783514507214849', 
                    '1209126038292123650',
                    '1210903150317494274',
                    '1214471918163460097',
                    '1215642304343425026',
                    '1226878328587288578')) a
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_repayment_plan_df
                    where dt='{pt}') b
                    on a.order_id=b.order_id
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_contract_df 
                    where dt='{pt}') x
                    on a.order_id=x.order_id
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_order_relate_user_df
                    where dt='{pt}') c
                    on a.order_id=c.order_id
                    left join 
                    (select order_id,
                            DATE_FORMAT(from_unixtime(UNIX_TIMESTAMP(create_time)+3600),'yyyy-MM-dd HH:mm:00') passtime 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_order_audit_history_df 
                    where dt='{pt}'
                    and audit_type=2 
                    and audit_status=1) f
                    on a.order_id=f.order_id
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_financial_product_phone_df
                    where dt='{pt}') g
                    on a.f_product_id=g.id
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_merchant_store_opay_info_df
                    where dt='{pt}') h
                    on a.merchant_id=h.merchant_store_id
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_t_repayment_detail_df 
                    where current_period=1
                    and dt='{pt}') j
                    on a.order_id=j.order_id
                    left join
                    (select a.user_id,
                            b.simple_name,
                            b.full_name 
                    from (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_sys_user_df 
                    where dt='{pt}') a
                    left join 
                    (select * 
                    from ocredit_phones_dw_ods.ods_sqoop_base_sys_dept_df
                    where dt='{pt}') b
                    on a.dept_id=b.dept_id) k
                    on a.user_id=k.user_id
        '''.format(pt=ds, ld=airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))

    logging.info('Executing: %s', sales_statistics)
    cursor.execute(sales_statistics)
    rows1 = cursor.fetchall()

    logging.info('Executing: %s', loan_amount_detail)
    cursor.execute(loan_amount_detail)
    rows2 = cursor.fetchall()

    file_name1 = '/tmp/sales_statistics_{dt}.xls'.format(dt=ds)
    file_name2 = '/tmp/loan_amount_detail_{dt}.xls'.format(dt=ds)

    # 生成excel文件
    book1 = xlwt.Workbook()
    sales_statistics = book1.add_sheet('sales_statistics', cell_overwrite_ok=True)

    book2 = xlwt.Workbook()
    loan_amount_detail = book2.add_sheet('loan_amount_detail', cell_overwrite_ok=True)

    # 表头标题
    sales_statistics.write(0, 0, '订单号')
    sales_statistics.write(0, 1, '进件日期')
    sales_statistics.write(0, 2, '进件时间')
    sales_statistics.write(0, 3, 'SA_code/销售人员代码')
    sales_statistics.write(0, 4, 'SA_Name/销售人员姓名')
    sales_statistics.write(0, 5, 'DMS名称')
    sales_statistics.write(0, 6, 'regional_manger')
    sales_statistics.write(0, 7, '商品类型')
    sales_statistics.write(0, 8, '信审通过时间')
    sales_statistics.write(0, 9, '商户ID')
    sales_statistics.write(0, 10, '商户名称')
    sales_statistics.write(0, 11, '门店ID')
    sales_statistics.write(0, 12, '门店名称')
    sales_statistics.write(0, 13, '审核结果')
    sales_statistics.write(0, 14, '首付成功时间')
    sales_statistics.write(0, 15, '资料合同上传时间')
    sales_statistics.write(0, 16, '合同审核通过时间')
    sales_statistics.write(0, 17, '首付金额')
    sales_statistics.write(0, 18, '贷款总额')

    # 表头标题
    loan_amount_detail.write(0, 0, '订单号')
    loan_amount_detail.write(0, 1, '进件日期')
    loan_amount_detail.write(0, 2, '进件时间')
    loan_amount_detail.write(0, 3, 'Region/地区')
    loan_amount_detail.write(0, 4, '商户ID')
    loan_amount_detail.write(0, 5, '商户名称')
    loan_amount_detail.write(0, 6, 'POS/门店代码')
    loan_amount_detail.write(0, 7, 'POS Name/门店名称')
    loan_amount_detail.write(0, 8, 'SA_code/销售人员代码')
    loan_amount_detail.write(0, 9, 'DMS名称')
    loan_amount_detail.write(0, 10, 'regional_manger')
    loan_amount_detail.write(0, 11, 'SA_Name/销售人员姓名')
    loan_amount_detail.write(0, 12, '审核通过时间')
    loan_amount_detail.write(0, 13, '客户姓名')
    loan_amount_detail.write(0, 14, '客户电话')
    loan_amount_detail.write(0, 15, '城市')
    loan_amount_detail.write(0, 16, '商品类型')
    loan_amount_detail.write(0, 17, '合同编号')
    loan_amount_detail.write(0, 18, '商品价格')
    loan_amount_detail.write(0, 19, '合同金额')
    loan_amount_detail.write(0, 20, '贷款总额')
    loan_amount_detail.write(0, 21, '业务类型')
    loan_amount_detail.write(0, 22, '分期数')
    loan_amount_detail.write(0, 23, '综合月费率')
    loan_amount_detail.write(0, 24, '首付金额')
    loan_amount_detail.write(0, 25, '首付比例')
    loan_amount_detail.write(0, 26, '总还款金额')
    loan_amount_detail.write(0, 27, '放款日期')
    loan_amount_detail.write(0, 28, '放款时间')
    loan_amount_detail.write(0, 29, '还款方式')
    loan_amount_detail.write(0, 30, '收款账户')
    loan_amount_detail.write(0, 31, '开始还款时间')
    loan_amount_detail.write(0, 32, '每月应收本金')
    loan_amount_detail.write(0, 33, '每月应收服务费')
    loan_amount_detail.write(0, 34, '每月还款总额')

    # 每一列写入excel文件，不然数据会全在一个单元格中
    for i in range(len(rows1)):
        for j in range(19):
            # print (rows[i][j])-
            # print ("--------")
            sales_statistics.write(i + 1, j, rows1[i][j])

    # 每一列写入excel文件，不然数据会全在一个单元格中
    for i in range(len(rows2)):
        for j in range(35):
            # print (rows2[i][j])-
            # print ("--------")
            loan_amount_detail.write(i + 1, j, rows2[i][j])

    book1.save(file_name1)
    book2.save(file_name2)
    cursor.close()

    # send mail

    #email_to = Variable.get("phone_sales_loan_detail").split()
    email_to = ['lili.chen@opay-inc.com']
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']

    email_subject = 'ocredit手机销售放款明细数据{dt}'.format(dt=ds)
    email_body = ''
    send_email(email_to, email_subject, email_body, [file_name1,file_name2], mime_charset='utf-8')


phone_sales_loan_detail_email = PythonOperator(
    task_id='phone_sales_loan_detail_email',
    python_callable=send_phone_sales_loan_detail_email,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_t_order_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_order_down_payment_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_contract_pic_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_contract_df_task >> phone_sales_loan_detail_email

ods_sqoop_base_t_order_audit_history_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_sys_user_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_sys_dept_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_repayment_plan_df_task >> phone_sales_loan_detail_email

ods_sqoop_base_t_order_relate_user_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_financial_product_phone_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_merchant_store_opay_info_df_task >> phone_sales_loan_detail_email
ods_sqoop_base_t_repayment_detail_df_task >> phone_sales_loan_detail_email