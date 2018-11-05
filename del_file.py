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



#连接数据库
def get_mysql_conn(host, port=3306, user="", passwd=""):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd,cursorclass = pymysql.cursors.DictCursor )
    except pymysql.Error as e:
        logger.error("Get connection fail, host: %s,port:%s,Msg :(%s,%s)" % (host, port, e.args[0], e.args[1]))
        return None
    return conn



#判断路径类型,是binlog还是普通文件
file_path='/home/q/mysql/3306/binlog/'
def get_path_type(file_path):
    #print(file_path)
    binlog_pattern = re.compile('binlog')
    port_pattern=re.compile('\d{4}')
    if port_pattern.search(file_path) and binlog_pattern.search(file_path)
        return 'binlog'
    elif not port_pattern.search(file_path):
        return 'ordinary'





#获取mysql实例端口
def get_mysql_port():
    if  get_path_type(file_path) == 'binlog':
        port_pattern = re.compile('\d{4}')
        res=port_pattern.search(file_path)
        mysql_port=res.group()
        return mysql_port
    else:
        return None



#判断集群类型
def get_cluster_type():
    if get_mysql_port() and get_mysql_port() is not None:
        mysql_port=get_mysql_port()
    conn = pymysql.connect(host=local_host, port=mysql_port, user=local_user, passwd=local_password)
    with conn as cursor:
        cursor.execute('show variables like \'semi_sync\'')
        if cursor.fetchone() is not None:
            return 'QMHA'
        cursor.execute('show variables like \'wsrep%\'')
        if cursor.fetchone() is not None:
            return 'PXC'
    return 'UNKOWN'

#删除普通文件
def del_ordinary_file():
    if get_path_type(file_path) == 'ordinary':
        del_cmd='rm -rf %s'%(file_path)
        (stat,re)=subprocess.getstatusoutput(del_cmd)
        if stat 



#del_ordinary_file()


# def del_file():
#     if get_path_type(file_path):
#         print(file_path)

# get_mysql_port()
# get_cluster_type()

get_path_type(file_path)