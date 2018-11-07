#!/usr/bin/env python35
# -*- coding: UTF-8 -*-
#function:获取执行任务

import pymysql
import json
import os.path
import logging
import subprocess
import re
import socket
from del_file import del_file


global hostname,ip
hostname=socket.gethostname()
ip = socket.gethostbyname(hostname)


global qdc_host,qdc_port,qdc_user,qdc_password
#qdc_host='l-qdcdb1.dba.cn8'
#qdc_port=3306
#qdc_user='dba_agent'
#qdc_password='qsgXzYMkTx8WBcRE'
qdc_host='l-jinhldb2.dba.dev.cn0'
qdc_port=3307
qdc_user='hailan.jin'
qdc_password='7SqDiaCZXedstHbI'




logger = logging.getLogger('agent_logger')







#连接数据库
def get_mysql_conn(host, port=3306, user="", passwd=""):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd,cursorclass = pymysql.cursors.DictCursor )
    except pymysql.Error as e:
        logger.error("Get connection fail, host: %s,port:%s,Msg :(%s,%s)" % (host, port, e.args[0], e.args[1]))
        return None
    return conn


def get_task():
#获取要执行的任务
    qdc_conn=get_mysql_conn(qdc_host,qdc_port,qdc_user,qdc_password)
    if qdc_conn == None:
        return 0
    qdc_cursor=qdc_conn.cursor()
    sql='select id,command from command where hostname=%s;'%(hostname)
    try:
        qdc_cursor.execute(sql)
        rows=qdc_cursor.fetchall()
    except  pymysql.Error as e :
        logger.error("Execute SQL fail, host: %s, port: %s, SQL: '%s', Msg: (%s, %s)" % (qdc_host, qdc_port, sql, e.args[0], e.args[1]))

    for task  in rows:
        task_line = json.loads(task['command'])
        task_dict=task_line[0]
        if "path" in task_dict and "type" in task_dict and task_dict['type'] == 'delete file':
            file_path=task_dict["path"]
            del_file(file_path)

















