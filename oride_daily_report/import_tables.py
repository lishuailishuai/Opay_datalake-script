# -*- coding: utf-8 -*-
import os
from utils.connection_helper import get_hive_cursor
file_path = os.path.realpath(__file__)
tmp_arr = file_path.split("/")
tmp_arr[-1] = "sqoop_db.sh"
sqoop_path = "/".join(tmp_arr)

default_command = '''
/usr/bin/sh %s %s %s
'''

tables = [
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
    "data_user_recharge"
]

def import_table(**op_kwargs):
    dt = op_kwargs.get('ds')
    print("running date: %s" % dt)
    cursor = get_hive_cursor()
    for table in tables:
        print("importing table: %s" % table)
        os.system(default_command % (sqoop_path, table, dt))
        cursor.execute("ALTER TABLE oride_db.%s ADD IF NOT EXISTS PARTITION (dt = '%s')" % (table, dt))
    print("import done")
