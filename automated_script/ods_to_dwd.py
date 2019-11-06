# coding=utf-8
import sys
import getopt
from string import Template
import os

def file_get_contents(filename):
    with open(filename) as f:
        return f.read()

def main(argv):
    owner = ""
    ods_path = ""
    table_name = ""
    table_comment = ""
    hdfs_path_base = ""
    hive_db = ""
    hive_ods_db = ""
    hive_ods_table = ""
    column_list = ""

    prompt_message = """
    python automated_script/ods_to_dwd.py \
        --owner=<owner> \
        --hive_db=<hive_db> \
        --table_name=<table_name> \
        --table_comment=<table_comment> \
        --hdfs_path_base=<hdfs_path_base> \
        --hive_ods_db=<hive_ods_db> \
        --hive_ods_table=<hive_ods_table> \
        --ods_path=<ods_path> \
        --column_list=<column_list>
    example:
    python automated_script/ods_to_dwd.py \
    --owner=zhenqian.zhang \
    --ods_path=oride_dw_sqoop/oride_data/data_driver_records_day \
    --table_name=dwd_oride_driver_records_day_df_test \
    --table_comment=中文 \
    --hdfs_path_base=ufile://opay-datalake/oride/oride_dw/ \
    --hive_db=oride_dw \
    --hive_ods_db=oride_dw_ods \
    --hive_ods_table=ods_sqoop_base_data_driver_records_day_df \
    --column_list=id,agenter_id,driver_id
    """

    try:
        """
            options, args = getopt.getopt(args, shortopts, longopts=[])

            参数args：一般是sys.argv[1:]。过滤掉sys.argv[0]，它是执行脚本的名字，不算做命令行参数。
            参数longopts：长格式分析串列表。例如：["help", "ip=", "port="]，help后面没有等号，表示后面不带参数；ip和port后面带冒号，表示后面带参数。
            返回值options是以元组为元素的列表，每个元组的形式为：(选项串, 附加参数)，如：('-i', '192.168.0.1')
            返回值args是个列表，其中的元素是那些不含'-'或'--'的参数。
        """
        opts, args = getopt.getopt(argv, "", [
            "help",
            "owner=",
            "ods_path=",
            "table_name=",
            "table_comment=",
            "hdfs_path_base=",
            "hive_db=",
            "hive_ods_db=",
            "hive_ods_table=",
            "column_list="
        ])
    except getopt.GetoptError:
        print('Error: %s' % prompt_message)
        sys.exit(2)

    # 处理 返回值options是以元组为元素的列表。
    for opt, arg in opts:
        if opt in ("--help"):
            print(prompt_message)
            sys.exit()
        elif opt in ("--owner"):
            owner = arg
        elif opt in ("--ods_path"):
            ods_path = arg
        elif opt in ("--table_name"):
            table_name = arg
        elif opt in ("--table_comment"):
            table_comment = arg
        elif opt in ("--hdfs_path_base"):
            hdfs_path_base = arg
        elif opt in ("--hive_db"):
            hive_db = arg
        elif opt in ("--hive_ods_db"):
            hive_ods_db = arg
        elif opt in ("--hive_ods_table"):
            hive_ods_table = arg
        elif opt in ("--column_list"):
            column_list = arg

    '''
    print('owner:', owner)
    print('ods_path:', ods_path)
    print('table_name:', table_name)
    print('table_comment:', table_comment)
    print('hdfs_path_base:', hdfs_path_base)
    print('hive_db:', hive_db)
    print('hive_ods_db:', hive_ods_db)
    print('hive_ods_table:', hive_ods_table)
    print('column_list:', column_list)
    '''

    # 验证输入数据
    if len(owner.strip())==0 or len(ods_path.strip())==0 or len(table_name.strip())==0 or len(table_comment.strip())==0 or len(hdfs_path_base.strip())==0 or len(hive_db.strip())==0 or len(hive_ods_db.strip())==0 or len(hive_ods_table.strip())==0 or len(column_list.strip())==0:
        print(prompt_message)
        sys.exit()

    # 生成文件
    content = file_get_contents('automated_script/ods_to_dwd_template')

    #print(content)
    s=Template(content)
    d={
        'owner':owner,
        'ods_path':ods_path,
        'table_name':table_name,
        'table_comment':table_comment,
        'hdfs_path_base':hdfs_path_base,
        'hive_db':hive_db,
        'hive_ods_db':hive_ods_db,
        'hive_ods_table':hive_ods_table,
        'column_list':column_list
    }

    path=hive_db+"_code/dwd"
    file=path+"/"+table_name+".py"
    if not os.path.exists(path):
        os.makedirs(path)
    f = open(file, 'w')
    f.write(s.substitute(d))
    f.close()
    print("create new dag file: %s" % file)

if __name__ == "__main__":
    # sys.argv[1:]为要处理的参数列表，sys.argv[0]为脚本名，所以用sys.argv[1:]过滤掉脚本名。
    main(sys.argv[1:])


"""
python automated_script/ods_to_dwd.py \
--owner=zhenqian.zhang \
--hive_db=oride_dw \
--table_name=dwd_oride_driver_records_day_df_test \
--table_comment=中文 \
--hdfs_path_base=ufile://opay-datalake/oride/oride_dw/ \
--hive_ods_db=oride_dw_ods \
--hive_ods_table=ods_sqoop_base_data_driver_records_day_df \
--ods_path=oride_dw_sqoop/oride_data/data_driver_records_day \
--column_list=id,agenter_id,driver_id
"""
