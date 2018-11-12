#!/usr/bin/env python35
# -*- coding: UTF-8 -*-
# function:删除文件

import pymysql
import json
import os.path
import logging
import subprocess
import re
import socket
import time
logger = logging.getLogger('agent_logger')

global local_host, local_user, local_password
local_host = '127.0.0.1'
local_password = 'xDvmgk67izldscTs'
local_user = 'dba'

file_path = '/home/q/mysql/multi/3307/binlog'
DEFAULT_PURGE_NUM = 20
DEFAULT_MIN_NUM = 50


# 连接数据库
def get_mysql_conn(host, port, user="", passwd=""):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, cursorclass=pymysql.cursors.DictCursor)
    except pymysql.Error as e:
        logger.error("Get connection fail, host: %s,port:%s,Msg :(%s,%s)" % (host, port, e.args[0], e.args[1]))
        return None
    return conn


# 判断路径类型,是binlog还是普通文件

def get_path_type(file_path):
    # print(file_path)
    binlog_pattern = re.compile('binlog')
    port_pattern = re.compile('\d{4}')
    if port_pattern.search(file_path) and binlog_pattern.search(file_path):
        return 'binlog'
    elif not port_pattern.search(file_path):
        return 'ordinary'


# 获取mysql实例端口
def get_mysql_port(file_path):
    if get_path_type(file_path) == 'binlog':
        print(file_path)
        port_pattern = re.compile('\d{4}')
        res = port_pattern.search(file_path)
        mysql_port = int(res.group())
        print(mysql_port)

        return mysql_port


# 判断集群删除方法
def get_cluster_type(file_path):
    try:
        if get_mysql_port(file_path) is not None:
            mysql_port = get_mysql_port(file_path)
    except:
        logger.error("get mysql instance port error!")

    conn = pymysql.connect(host=local_host, port=mysql_port, user=local_user, passwd=local_password)
    cursor = conn.cursor()
    cursor.execute('show variables like \'wsrep%\'')
    if cursor.fetchone() is not None:
        return 'pxc'
    else:
        cursor.fetchall()


# # 获取binlog个数,所有binlog
# def get_binlog_info(file_path,num):
#     if get_path_type(file_path) != 'binlog':
#         return
#
# binlog_pattern = 'mysql-bin\\.\d{6}'
# binlog_list = []
# for item in os.listdir(file_path):
#     if re.match(binlog_pattern, item):
#        binlog_list.append(item)
#     else:
#         continue
# binlog_list = sorted(binlog_list)
# # if num <= DEFAULT_MIN_NUM:
# #     return False
# actual_num = num if len(out) - num >= DEFAULT_MIN_NUM else len(
#     out) - DEFAULT_MIN_NUM
# return num, binlog_list

# 获取binlog列表
def get_binlog_list(file_path):
    binlogs = subprocess.getoutput('cat {file_path}/mysql-bin.index'.format(file_path))
    binlog_list = binlogs.split('\n')
    return binlog_list


# 判断能否删除binlog
def check_deletable(num):
    binlog_list = get_path_type(file_path)
    if len(binlog_list) - num >= DEFAULT_MIN_NUM:
        return True
    else:
        return False


# 获取要删除的binlog信息
def get_binlog_info(num):
    if get_path_type(file_path) != 'binlog':
        return False
    binlog_list = get_path_type(file_path)
    if check_deletable(num) is False:
        return False
    del_num = len(binlog_list) - DEFAULT_MIN_NUM
    return binlog_list[:del_num]




# 删除普通文件
def del_ordinary_file(file_path):
    del_cmd = 'rm -rf %s' % (file_path)
    (stat, re) = subprocess.getstatusoutput(del_cmd)
    if stat == 0:
        return True
    if stat == 1:
        return False




# 删除binlog，默认删除20个
def remove_binlog(file_path):
    del_binlog_list = get_binlog_info(file_path)
    os.chdir(file_path)
    for binlog in del_binlog_list :
        os.remove(binlog)
        time.sleep(1)

# 清空binlog
def purge_to_binlog(num,index_path):
    subprocess.getoutput('sed -i \'1, {number}d\' {index_path}/mysql-bin.index'.format(number=num,file_path=file_path))



def del_file(file_path):
    if get_path_type(file_path) == 'ordinary':
        # del_ordinary_file(file_path)
        print('del_file now ')
        if del_ordinary_file(file_path):
            print('remove file succees')
            return 'succeed'
    if get_path_type(file_path) == 'binlog' and get_cluster_type(file_path) == 'pxc':
        if not purge_binlog(file_path):
            return False
        return True
