#!/usr/bin/env python35
# -*- coding: UTF-8 -*-
#function:删除文件

import pymysql
import  json
import os.path
import logging
import subprocess
import re
import socket



global local_host,local_user,local_password
local_host='127.0.0.1'
local_password='xDvmgk67izldscTs'
local_user='dba'

logger = logging.getLogger('agent_logger')

min_num=20

#连接数据库
def get_mysql_conn(host, port, user="", passwd=""):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd,cursorclass = pymysql.cursors.DictCursor )
    except pymysql.Error as e:
        logger.error("Get connection fail, host: %s,port:%s,Msg :(%s,%s)" % (host, port, e.args[0], e.args[1]))
        return None
    return conn



#判断路径类型,是binlog还是普通文件

def get_path_type(file_path):
    #print(file_path)
    binlog_pattern = re.compile('binlog')
    port_pattern=re.compile('\d{4}')
    if port_pattern.search(file_path) and binlog_pattern.search(file_path):
        return 'binlog'
    elif not port_pattern.search(file_path):
        return 'ordinary'





#获取mysql实例端口
def get_mysql_port(file_path):
    if  get_path_type(file_path) == 'binlog':
        print(file_path)
        port_pattern = re.compile('\d{4}')
        res=port_pattern.search(file_path)
        mysql_port=int(res.group())
        print(mysql_port)

        return mysql_port




#判断集群类型
def get_cluster_type(file_path):
    try:
        if get_mysql_port(file_path) is not None:
            mysql_port = get_mysql_port(file_path)
    except :
        logger.error("get mysql instance port error!")

    conn = pymysql.connect(host=local_host, port=mysql_port, user=local_user, passwd=local_password)
    with conn as cursor:
        cursor.execute('show variables like \'semi_sync\'')
        if cursor.fetchone() is not None:
            return 'QMHA'
        cursor.execute('show variables like \'wsrep%\'')
        if cursor.fetchone() is not None:
            return 'PXC'
    return 'UNKOWN'



#获取binlog个数,所有binlog
def get_binlog_info(file_path):

    if get_path_type(file_path) != 'binlog':
        return

    binlog_pattern='mysql-bin\\.\d{6}'
    num=0
    binlog_list=[]
    for item in os.listdir(file_path):
        if re.match(binlog_pattern, item):
            num += 1
            binlog_list.append(item)
        else:
            continue
    binlog_list=sorted(binlog_list)
    if num <= min_num:
        return False
    return  num ,binlog_list


#判断是可以删除，默认保留20个binlog
# def check_binlog_available():
#     min_num=20
#     if not get_binlog_info():
#         return False
#     binlog_num=get_binlog_info()
#     if binlog_num <= min_num:
#         return  False
#     return True




#删除普通文件
def del_ordinary_file(file_path):

    del_cmd='rm -rf %s'%(file_path)
    (stat,re)=subprocess.getstatusoutput(del_cmd)
    if stat == 0:
        return True
    if stat == 1:
        return False


#清理PXC binlog文件
def del_pxc_binlog(file_path):
    if not get_binlog_info():
        return False
    (num,binlog_list)=get_binlog_info()
    local_binlog=binlog_list[19]
    local_port=get_mysql_port()
    local_con=get_mysql_conn(local_host, local_port, local_user,local_password)
    cursor=local_con.cursor()
    try:
        clean_binlog_sql="purge binary logs to '%s'"%(local_binlog)
        cursor.execute(clean_binlog_sql)
        cursor.close()
    except pymysql.Error as e:
        logger.error("Execute SQL fail ,host: %s, port: %s, SQL: '%s', Msg: (%s, %s)"%(local_host,local_port,clean_binlog_sql,e.args[0],e.args[1]))
        return False
    return True




def del_file(file_path):

    if get_path_type(file_path) == 'ordinary':
        #del_ordinary_file(file_path)
        print('del_file now ')
        if del_ordinary_file(file_path):
            print('remove file succees')
            return 'succeed'
    if get_path_type(file_path) == 'binlog' and get_cluster_type() == 'PXC':
        if not del_pxc_binlog(file_path) :
            return False
        return True

