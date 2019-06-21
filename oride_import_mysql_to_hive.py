import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_db_conf
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_import_mysql_to_hive',
    schedule_interval="0 0 * * *",
    default_args=args)

table_list = [
    "data_activity",
    "data_agenter_motorbike",
    "data_app_config",
    "data_billboard_config",
    "data_city_conf",
    "data_coupon",
    "data_coupon_log",
    "data_coupon_template",
    "data_coupons_template",
    "data_device",
    "data_driver",
    "data_driver_balance_extend",
    "data_driver_balance_records",
    "data_driver_comment",
    "data_driver_discount",
    "data_driver_extend",
    "data_driver_fee_blacklist",
    "data_driver_group",
    "data_driver_operation_log",
    "data_driver_pay_records",
    "data_driver_recharge_records",
    "data_driver_records_day",
    "data_driver_reward",
    "data_driver_reward_push",
    "data_fcm_template",
    "data_invite",
    "data_invite_conf",
    "data_motorbike",
    "data_motorbike_extend",
    "data_novice_coupons_conf",
    "data_opay_transaction",
    "data_order",
    "data_order_payment",
    "data_payconf",
    "data_promo_code",
    "data_recharge_conf",
    "data_recharge_options",
    "data_reward_conf",
    "data_role_invite",
    "data_sms_template",
    "data_user",
    "data_user_comment",
    "data_user_complaint",
    "data_user_extend",
    "data_user_recharge",
    "data_user_whitelist",
    "data_driver_whitelist",
    "data_user_blacklist",
    "data_driver_blacklist",
    "data_abnormal_order",
]

host, port, schema, login, password = get_db_conf('sqoop_db')
for table_name in table_list:
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/oride/db/{table}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(host=host, port=port, schema=schema, username=login, password=password,table=table_name),
        dag=dag,
    )
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table_name),
        hql='''
            ALTER TABLE oride_db.{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
        '''.format(table=table_name),
        schema='oride_source',
        dag=dag)


    import_table >> add_partitions
