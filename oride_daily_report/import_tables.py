# -*- coding: utf-8 -*-
from datetime import *
import time
import os
from oride_daily_report.connection_helper import get_hive_cursor

default_command = '''
/usr/bin/sh ./sqoop_db.sh %s %s
'''

tables = [
    "data_driver_extend", "data_order", "data_order_payment", "data_user_extend",
    "data_driver_reward"
]


def import_table(**op_kwargs):
    dt = op_kwargs.get('ds')
    print("running date: %s" % dt)
    cursor = get_hive_cursor()
    for table in tables:
        print("importing table: %s" % table)
        os.system(default_command % (table, dt))
        cursor.execute("msck repair table oride_source.db_%s" % table)
    print()
