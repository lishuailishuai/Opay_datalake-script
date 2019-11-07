import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('fill_data_temp',
                  schedule_interval="* * * * 9999",
                  default_args=args)

##------declare variables end ------##


##---- hive operator ---##
ods_sqoop_base_user_di = HiveOperator(
    task_id='ods_sqoop_base_user_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ods_sqoop_base_user_di 
    partition(dt)
    select 
    id,user_id,mobile,business_name,first_name,middle_name,surname,kyc_level,kyc_update_time,bvn,dob,gender,country,state,city,address,lga,role,referral_code,referrer_code,notification,create_time,update_time, 
    date_format(create_time, 'yyyy-MM-dd') dt
    from ods_sqoop_base_user_df 
    where dt ='2019-10-26';
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator ods_sqoop_base_adjustment_decrease_record_di ---##
ods_sqoop_base_adjustment_decrease_record_di = HiveOperator(
    task_id='ods_sqoop_base_adjustment_decrease_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table opay_dw_ods.ods_sqoop_base_adjustment_decrease_record_di 
    partition(dt)
    select 
        id,
        order_no,
        user_id,
        user_type,
        amount,
        country,
        currency,
        pay_channel,
        order_status,
        adjust_reason,
        original_order_no,
        bank_original_order_no,
        error_code,
        error_msg,
        fee_amount,
        fee_pattern,
        out_ward_id,
        out_ward_type,
        create_time,
        update_time,
        date_format(create_time, 'yyyy-MM-dd') dt
    from opay_dw_ods.ods_sqoop_base_adjustment_decrease_record_df 
    where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_adjustment_increase_record_di= HiveOperator(
    task_id='ods_sqoop_base_adjustment_increase_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table opay_dw_ods.ods_sqoop_base_adjustment_increase_record_di 
    partition(dt)
    select 
        id,
        order_no,
        user_id,
        user_type,
        amount,
        country,
        currency,
        pay_channel,
        order_status,
        adjust_reason,
        original_order_no,
        bank_original_order_no,
        error_code,
        error_msg,
        fee_amount,
        fee_pattern,
        out_ward_id,
        out_ward_type,
        create_time,
        update_time,
        date_format(create_time, 'yyyy-MM-dd') dt
    from opay_dw_ods.ods_sqoop_base_adjustment_increase_record_df 
    where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_airtime_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_airtime_topup_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table opay_dw_ods.ods_sqoop_base_airtime_topup_record_di 
    partition(dt)
    select 
        id,
        order_no,
        user_id,
        merchant_id,
        channel_order_no,
        amount,
        country,
        currency,
        recipient_mobile,
        telecom_perator,
        pay_channel,
        pay_status,
        order_status,
        error_code,
        error_msg,
        fee_amount,
        fee_pattern,
        out_ward_id,
        out_ward_type,
        recipient_email,
        current_balance,
        till_date_balance,
        audit_no,
        confirm_code,
        out_channel_order_no,
        out_channel_id,
        business_no,
        create_time,
        update_time,
        actual_pay_amount,
        date_format(create_time, 'yyyy-MM-dd') dt
    from opay_dw_ods.ods_sqoop_base_airtime_topup_record_df 
    where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_betting_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_betting_topup_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table opay_dw_ods.ods_sqoop_base_betting_topup_record_di 
    partition(dt)
    select 
        id,
        order_no,
        user_id,
        merchant_id,
        out_order_no,
        amount,
        country,
        currency,
        pay_channel,
        pay_status,
        order_status,
        fee_amount,
        fee_pattern,
        error_msg,
        betting_provider,
        recipient_betting_account,
        recipient_betting_name,
        outward_id,
        outward_type,
        out_channel_id,
        create_time,
        update_time,
        user_role,
        business_no,
        date_format(create_time, 'yyyy-MM-dd') dt
    from opay_dw_ods.ods_sqoop_base_betting_topup_record_df 
    where dt ='2019-10-26'

    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_electricity_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_electricity_topup_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_electricity_topup_record_di 
partition(dt)
select 
	id,
	order_no,
	user_id,
	merchant_id,
	channel_order_no,
	amount,
	country,
	currency,
	recipient_elec_account,
	recipient_elec_perator,
	electricity_payment_plan,
	pay_channel,
	pay_status,
	order_status,
	error_code,
	error_msg,
	account_type,
	fee_amount,
	fee_pattern,
	out_ward_type,
	out_ward_id,
	contract_type,
	recipient_email,
	current_balance,
	till_date_balance,
	customer_name,
	customer_district,
	customer_addr,
	customer_refrence_type,
	customer_account_type,
	customer_dt_no,
	thirdparty_code,
	unique_reference,
	unique_code,
	electricity_token,
	out_channel_order_no,
	out_channel_id,
	business_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_electricity_topup_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_acquiring_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_acquiring_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_acquiring_record_di 
partition(dt)
select 
	id,
	order_no,
	user_id,
	merchant_id,
	merchant_order_no,
	amount,
	country,
	currency,
	pay_channel,
	order_status,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	original_order_no,
	source_system,
	order_type,
	is_refund,
	error_msg,
	external_reason,
	internal_reason,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_acquiring_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_pos_transaction_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_pos_transaction_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_pos_transaction_record_di 
partition(dt)
select 
	id,
	order_no,
	terminal_id,
	pos_trade_req_id,
	transaction_reference,
	retrieval_reference_number,
	merchant_id,
	currency,
	amount,
	fee_amount,
	fee_pattern,
	channel_code,
	channel_msg,
	payment_date,
	transaction_type,
	order_status,
	accounting_status,
	terminal_provider_id,
	bank_code,
	country,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_pos_transaction_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_receive_money_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_receive_money_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_receive_money_record_di 
partition(dt)
select 
	id,
	order_no,
	order_type,
	merchant_id,
	user_mobile,
	amount,
	currency,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	bank_account_code,
	bank_account_name,
	scheme,
	order_status,
	fail_msg,
	accounting_status,
	channel_id,
	channel_order_no,
	out_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_receive_money_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_topup_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_topup_record_di 
partition(dt)
select 
	id,
	order_no,
	merchant_order_no,
	business_type,
	source_system,
	merchant_id,
	merchant_name,
	amount,
	currency,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	bank_card_no_encrypted,
	bank_card_no_desensitized,
	bank_account_no_encrypted,
	bank_account_no_desensitized,
	scheme,
	bank_code,
	bank_name,
	pay_channel,
	first_name,
	last_name,
	email,
	customer_phone,
	customer_email,
	token,
	order_status,
	next_step,
	auth_url,
	error_code,
	error_msg,
	accounting_status,
	out_channel_id,
	out_order_no,
	channel_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_topup_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_transfer_card_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_transfer_card_record_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_transfer_card_record_di 
partition(dt)
select 
	id,
	order_no,
	merchant_order_no,
	source_system,
	merchant_id,
	merchant_name,
	amount,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	currency,
	recipient_bank_code,
	recipient_bank_name,
	recipient_bank_account_no_encrypted,
	recipient_bank_account_no_desensitized,
	recipient_bank_account_name,
	recipient_kyc_level,
	recipient_bvn_encrypted,
	message,
	customer_phone,
	customer_email,
	pay_channel,
	order_status,
	error_code,
	error_msg,
	pay_status,
	out_channel_id,
	out_order_no,
	channel_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_transfer_card_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##


##---- hive operator  ---##
ods_sqoop_base_merchant_transfer_user_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_transfer_user_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_transfer_user_record_di 
partition(dt)
select 
	id,
	order_no,
	merchant_id,
	merchant_name,
	merchant_order_no,
	recipient_id,
	recipient_name,
	recipient_mobile,
	message,
	amount,
	country,
	currency,
	order_status,
	fee_amount,
	fee_pattern,
	outwardid,
	outwardtype,
	error_msg,
	recipient_type,
	create_time,
	update_time,
		date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_transfer_user_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_mobiledata_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_mobiledata_topup_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_mobiledata_topup_record_di 
partition(dt)
select 
	id,
	order_no,
	user_id,
	merchant_id,
	channel_order_no,
	amount,
	country,
	currency,
	recipient_mobile,
	telecom_perator,
	pay_channel,
	pay_status,
	order_status,
	error_code,
	error_msg,
	fee_amount,
	fee_pattern,
	out_ward_id,
	out_ward_type,
	recipient_email,
	current_balance,
	till_date_balance,
	audit_no,
	confirm_code,
	out_channel_id,
	out_channel_order_no,
	business_no,
	create_time,
	update_time,
			date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_mobiledata_topup_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_receive_money_request_record_di= HiveOperator(
    task_id='ods_sqoop_base_receive_money_request_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_receive_money_request_record_di 
partition(dt)
select 
	id,
	user_or_merchant_id,
	user_or_merchant_type,
	session_id,
	order_no,
	scheme,
	account_number,
	beneficiary_account_name,
	beneficiary_account_number,
	beneficiary_bank_verification_number,
	beneficiary_kyc_level,
	originator_account_name,
	originator_account_number,
	originator_bank_verification_number,
	originator_kyc_level,
	transaction_location,
	narration,
	payment_reference,
	amount,
	transaction_fee,
	mandate_reference_number,
	bank_name,
	create_time,
	update_time,
				date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_receive_money_request_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_transfer_not_register_record_di= HiveOperator(
    task_id='ods_sqoop_base_transfer_not_register_record_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_transfer_not_register_record_di 
partition(dt)
select 
	id,
	order_no,
	recipient_mobile,
	amount,
	country,
	currency,
	pending_transfer_status,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_transfer_not_register_record_df 
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_tv_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_tv_topup_record_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_tv_topup_record_di
partition(dt)
select 
	id,
	order_no,
	user_id,
	user_name,
	merchant_id,
	amount,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	currency,
	recipient_tv_account_no,
	recipient_tv_account_name,
	tv_provider,
	tv_plan,
	basket_id,
	mobile,
	pay_channel,
	order_status,
	pay_status,
	error_code,
	error_msg,
	out_channel_id,
	out_order_no,
	channel_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_tv_topup_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_easycash_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_easycash_record_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_easycash_record_di
partition(dt)
select 
	id,
    order_no,
    user_id,
    bank_code,
    amount,
    fac,
    mobile,
    bank_account_no,
    order_status,
    currency,
    country,
    remark,
    channel_name,
    channel_mobile,
    next_step,
    request_id,
    accounting_status,
    fee_amount,
    fee_pattern,
    verify_state,
    date_of_birth,
    first_name,
    last_name,
    error_code,
    error_msg,
    out_order_no,
    channel_order_no,
    create_time,
    update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_easycash_record_df
where dt ='2019-10-29'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_pos_transaction_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_pos_transaction_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_pos_transaction_record_di
partition(dt)
select 
	id,
	order_no,
	pos_trade_req_id,
	transaction_reference,
	retrieval_reference_number,
	terminal_id,
	user_id,
	currency,
	amount,
	fee_pattern,
	order_status,
	accounting_status,
	channel_code,
	channel_msg,
	payment_date,
	fee_amount,
	transaction_type,
	terminal_provider_id,
	bank_code,
	country,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_pos_transaction_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_receive_money_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_receive_money_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_receive_money_record_di
partition(dt)
select 
	id,
	order_no,
	order_type,
	user_id,
	user_mobile,
	amount,
	currency,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	bank_account_code,
	bank_account_name,
	scheme,
	order_status,
	fail_msg,
	accounting_status,
	channel_id,
	channel_order_no,
	out_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_receive_money_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_topup_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_topup_record_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_topup_record_di
partition(dt)
select 
	id,
	order_no,
	user_id,
	user_name,
	user_mobile,
	amount,
	currency,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	bind_card_id,
	bank_card_no_encrypted,
	bank_card_no_desensitized,
	bank_account_no_encrypted,
	bank_account_no_desensitized,
	scheme,
	bank_code,
	bank_name,
	pay_channel,
	order_status,
	next_step,
	auth_url,
	error_code,
	error_msg,
	accounting_status,
	out_channel_id,
	out_order_no,
	channel_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_topup_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_transfer_card_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_transfer_card_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_transfer_card_record_di
partition(dt)
select 
	id,
	order_no,
	user_id,
	user_name,
	user_mobile,
	user_kyc_level,
	amount,
	fee,
	fee_pattern,
	outward_id,
	outward_type,
	country,
	currency,
	recipient_bank_code,
	recipient_bank_name,
	recipient_bank_account_no_encrypted,
	recipient_bank_account_no_desensitized,
	recipient_bank_account_name,
	recipient_kyc_level,
	recipient_bvn_encrypted,
	message,
	pay_channel,
	order_status,
	error_code,
	error_msg,
	pay_status,
	out_channel_id,
	out_order_no,
	channel_order_no,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_transfer_card_record_df
where dt ='2019-10-26'

    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_transfer_user_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_transfer_user_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table opay_dw_ods.ods_sqoop_base_user_transfer_user_record_di
    partition(dt)
    select 
        id,
        order_no,
        user_id,
        user_name,
        recipient_id,
        recipient_name,
        recipient_mobile,
        message,
        amount,
        country,
        currency,
        transfer_status,
        fee_amount,
        fee_pattern,
        outwardid,
        outwardtype,
        error_msg,
        create_time,
        update_time,
        recipient_opay_account,
        recipient_type,
        date_format(create_time, 'yyyy-MM-dd') dt
    from opay_dw_ods.ods_sqoop_base_user_transfer_user_record_df
    where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_big_order_di= HiveOperator(
    task_id='ods_sqoop_base_big_order_di',
    hql='''
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_big_order_di
partition(dt)
select 
    id,
	order_no,
	order_user_type,
	user_id,
	amount,
	fee_pattern,
	fee_amount,
	country,
	currency,
	order_status,
	merchant_id,
	service_type,
	pay_channel,
	business_type,
	other_trader,
	other_trader_user_type,
	after_balance,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_big_order_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_email_di= HiveOperator(
    task_id='ods_sqoop_base_user_email_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_email_di
partition(dt)
select 
	id,
	user_id,
	email,
	email_verified,
	email_to_verify,
	email_verification_id,
	email_verification_date,
	email_verification_cooldown,
	email_verification_last_sent,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_email_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_message_record_di= HiveOperator(
    task_id='ods_sqoop_base_message_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_message_record_di
partition(dt)
select 
	id,
	template_name,
	country_code,
	message_type,
	mobile,
	content,
	params,
	language,
	message_channels,
	retry_times ,
	delivered_channel,
	status,
	third_msg_id,
	remark,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_message_record_df
where dt ='2019-10-26'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_user_fee_record_di= HiveOperator(
    task_id='ods_sqoop_base_user_fee_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_user_fee_record_di
partition(dt)
select 
	id,
	order_no,
	amount,
	fee,
	currency,
	user_id,
	user_fee_rate_id,
	fee_pattern,
	outward_type,
	outward_id,
	inward_type,
	inward_id,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_user_fee_record_df
where dt ='2019-10-29'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_merchant_fee_record_di= HiveOperator(
    task_id='ods_sqoop_base_merchant_fee_record_di',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table opay_dw_ods.ods_sqoop_base_merchant_fee_record_di
partition(dt)
select 
	id,
	order_no,
	amount,
	fee,
	currency,
	merchant_id,
	merchant_fee_rate_id,
	fee_pattern,
	outward_type,
	outward_id,
	inward_type,
	inward_id,
	create_time,
	update_time,
	date_format(create_time, 'yyyy-MM-dd') dt
from opay_dw_ods.ods_sqoop_base_merchant_fee_record_df
where dt ='2019-10-29'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator  ---##
ods_sqoop_base_business_collection_record_di= HiveOperator(
    task_id='ods_sqoop_base_business_collection_record_di',
    hql='''
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table opay_dw_ods.ods_sqoop_base_business_collection_record_di
        partition(dt)
        select 
            id,
            order_no,
            platform_order_no,
            platform_id,
            platform_name,
            sender_id,
            sender_type,
            sender_name,
            sender_mobile,
            recipient_id,
            recipient_type,
            recipient_name,
            recipient_mobile,
            amount,
            currency,
            fee,
            fee_pattern,
            outward_id,
            outward_type,
            country,
            pay_channel,
            message,
            order_status,
            error_code,
            error_msg,
            create_time,
            update_time,
            date_format(create_time, 'yyyy-MM-dd') dt
        from opay_dw_ods.ods_sqoop_base_business_collection_record_df
        where dt ='2019-10-29'
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw_ods',
    dag=dag
)
##---- hive operator end ---##
ods_sqoop_base_adjustment_decrease_record_di
ods_sqoop_base_adjustment_increase_record_di
ods_sqoop_base_airtime_topup_record_di
ods_sqoop_base_betting_topup_record_di
ods_sqoop_base_electricity_topup_record_di
ods_sqoop_base_merchant_acquiring_record_di
ods_sqoop_base_merchant_pos_transaction_record_di
ods_sqoop_base_merchant_receive_money_record_di
ods_sqoop_base_merchant_topup_record_di
ods_sqoop_base_merchant_transfer_card_record_di
ods_sqoop_base_merchant_transfer_user_record_di
ods_sqoop_base_mobiledata_topup_record_di
ods_sqoop_base_receive_money_request_record_di
ods_sqoop_base_transfer_not_register_record_di
ods_sqoop_base_tv_topup_record_di
ods_sqoop_base_user_easycash_record_di
ods_sqoop_base_user_pos_transaction_record_di
ods_sqoop_base_user_receive_money_record_di
ods_sqoop_base_user_topup_record_di
ods_sqoop_base_user_transfer_card_record_di
ods_sqoop_base_user_transfer_user_record_di
ods_sqoop_base_big_order_di
ods_sqoop_base_user_email_di
ods_sqoop_base_message_record_di
ods_sqoop_base_user_fee_record_di
ods_sqoop_base_merchant_fee_record_di
ods_sqoop_base_business_collection_record_di