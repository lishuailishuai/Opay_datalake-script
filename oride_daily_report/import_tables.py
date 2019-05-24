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
    "data_driver_extend", "data_order", "data_order_payment", "data_user_extend",
    "data_driver_reward", "db_data_coupon"
]


def import_table(**op_kwargs):
    dt = op_kwargs.get('ds')
    print("running date: %s" % dt)
    cursor = get_hive_cursor()
    for table in tables:
        print("importing table: %s" % table)
        os.system(default_command % (sqoop_path, table, dt))
        cursor.execute("msck repair table oride_source.db_%s" % table)
    print("import done")
