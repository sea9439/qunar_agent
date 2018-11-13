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
        logger.error("msyql binlog is not deletable!")
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
    del_binlog_list = get_binlog_info(DEFAULT_PURGE_NUM)
    os.chdir(file_path)

    for binlog in del_binlog_list :
        os.remove(binlog)
        (status,info)=subprocess.getstatusoutput(
            'sed -i \'/{log}/d\' {index_path}/mysql-bin.index'.format(index_path=file_path,log=binlog))
        if status not in [0,1] :
            logger.error("remove mysql binlog error! error info:" + info)
            return False
    return True


def del_file(file_path):
    if get_path_type(file_path) == 'ordinary':
        # del_ordinary_file(file_path)
        print('del_file now ')
        if del_ordinary_file(file_path):
            print('remove file succees')
            return 'succeed'
    if get_path_type(file_path) == 'binlog' :
        check_deletable(DEFAULT_PURGE_NUM)

        remove_binlog(file_path)
            




