#!/bin/bash

#------------------------------------------公共变量说明 ------------------------------------------#

#对应北京的最小时间(YYYYmmddHH)
#V_P_BEGIN_TIME

#对应北京的最大时间(YYYYmmddHH)
#V_P_END_TIME

# alter add 变量
#V_P_ALTER_ADD_ARR

# alter drop 变量
#V_P_ALTER_DROP_ARR

#用于 where 条件，包含二位、三位国家码('CA','CAN','US','USA')
#V_P_M_COUNTRY_CODE_WHERE

#用于 where 条件，只包含二位国家码('CA','US')
#V_P_COUNTRY_CODE_WHERE

# 数据库
#V_P_DB_CODE

#数据路径
#V_P_HDFS_DATA_PATH

#年月日
#V_P_PARYEAR
#V_P_PARMONTH
#V_P_PARDAY

#获取表中的全部字段，用于PI 隔离
#V_P_TAB_SCHEME

#过滤条件(分为全量和增量)
#V_P_WHERE_KEY

#life:城市时区，capital 首都时区
#V_BUI_TYPE

#--------------------------------------- 外 参 ------------------------------------------#

Usage() {

    echo """
        #### 提示 ####

        ./ShellFile.sh br,mx,jp life 2019010715

    """
} 


#国家码
V_COUNTRY_CODE=$1  #--(修改)

#业务类型
V_BUI_TYPE=$2

if [[ -z "$V_BUI_TYPE" ]];then
    Usage
    echo "BUSINESS TYPE (life/capital) Is Empty ......"
    exit 1
fi

V_DATE=$3

#------------------------------------以下变量必填项--------------------------------------------#

#依赖的数据源
INPUT_TABLE_NAME="international.dwd_finance_u_user_trans_hi"

#填写表名中间部分 ，前后命名由系统完成。目前只支持dim、dwd、dwm、ods数据表
OUTPUT_TABLE_MID_NAME_KEY="finance_u_user_trans"

#inc/whole/loginc
TABLE_UPDATE_TYPE="inc"

#国家码字段(不存在就置空)
COUNTRY_COADE_KEY="country_code"

#城市字段(不存在就置空)
CITY_KEY="order_area"

#产品线ID字段(不存在就置空)
PRODUCT_ID_KEY="product_id"

#表中主键字段，多个字段用逗号分隔
PRIVATRE_KEY="billid"

#加工类型(dwd,dwm,dim,dm,binlog,public),天级表开头的命名，以PROCESS_TYPE 为准
#除 binlog,public 表的开头命名为ods，其它以PROCESS_TYPE 为准
PROCESS_TYPE="dwd"

#自定义hive set 参数
CUSTOM_HIVE_SET=""

#自定义数据产出标识类型(true,false,need),默认：true
#false：允许所有国家产出的数据为空
#true：允许新国家产出的数据为空
#need：不允许数据为空(所有国家)
#参数不使用时为默认类型true
CHECK_COMPLETE_TYPE=""

#--------------------------------------- 执行脚本 ----------------------------------------------#


sh public_template_frame.sh -c "${V_COUNTRY_CODE}" -b "${V_BUI_TYPE}" -d "${V_DATE}" -i "${INPUT_TABLE_NAME}" -u "${TABLE_UPDATE_TYPE}" -n "${COUNTRY_COADE_KEY}"  -y "${CITY_KEY}" -p "${PRODUCT_ID_KEY}" -r "${PRIVATRE_KEY}" -e "${PROCESS_TYPE}" -m "${OUTPUT_TABLE_MID_NAME_KEY}" -s "${CUSTOM_HIVE_SET}" -k "${CHECK_COMPLETE_TYPE}"
