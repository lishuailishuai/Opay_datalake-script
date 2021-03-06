# -*- coding: utf-8 -*-
import os
from utils.connection_helper import get_hive_cursor, get_db_conf

file_path = os.path.realpath(__file__)
tmp_arr = file_path.split("/")
tmp_arr[-1] = "sqoop_db.sh"
sqoop_path = "/".join(tmp_arr)

default_command = '''
/usr/bin/sh %s %s %s %s %s %s %s %s
'''

tables = [
    "data_activity",
    "data_agenter_motorbike",
    "data_app_config",
    "data_billboard_config",
    "data_city_conf",
    "data_coupon_log",
    "data_coupon_template",
    "data_coupons_template",
    "data_device",
    "data_driver_balance_extend",
    "data_driver_balance_records",
    "data_driver_discount",
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
    "data_payconf",
    "data_promo_code",
    "data_recharge_conf",
    "data_recharge_options",
    "data_reward_conf",
    "data_role_invite",
    "data_sms_template",
    "data_user_comment",
    "data_user_complaint",
    "data_user_recharge",
    "data_user_whitelist",
    "data_driver_whitelist",
    "data_user_blacklist",
    "data_driver_blacklist"
]


def import_table(**op_kwargs):
    dt = op_kwargs.get('ds')
    env = op_kwargs.get("env")
    print("running date: %s" % dt)
    cursor = get_hive_cursor()
    conf_name = "sqoop_db"
    if env == "test":
        conf_name += "_test"
    host, port, schema, login, password = get_db_conf(conf_name)
    host += ":" + str(port)
    for table in tables:
        print("importing table: %s" % table)
        hive_table = table
        if env == "test":
            hive_table += "_dev"
        os.system(default_command % (sqoop_path, host, schema, login, password, table, hive_table, dt))
        cursor.execute("ALTER TABLE oride_db.%s ADD IF NOT EXISTS PARTITION (dt = '%s')" % (hive_table, dt))
    print("import done")
