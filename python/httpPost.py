#!/usr/bin/env python
#  -*- coding:utf-8 -*-
# File httpPost.py

import urllib
import urllib2
import json
import sys
import requests
import MySQLdb
import time
import thread

postAlarms = [
    # from docker's alarms
    'Fail to clear log',
	'Can\'t find available port',
    # from dbrouter's alarms
    'EXECING_SLOW_SQL',
	'CONFIG_ERROR',
    'DB_CONN_BREAK',
	'DB_ACCOUNT',
	'DB_QUERY_FAIL',
	'DB_OTHER_ERROR',
    'SQL_TIMEOUT',
    'SQL_LONG_TIMEOUT',
]

watchUrl = 'http://tcopenapi.17usoft.com/Interface/Service/SendMessage?account=webMidNotify&password=webMidNotify&mobile=18301728499&message=watchdog_'
postUrl = 'http://unionmonitor.ops.17usoft.com/mointor/add'
myTeam = u"DBProxyTeam"
myReportor = u"DBRouter"
myLevel = 5

reload(sys)
sys.setdefaultencoding('utf8')

def hourly_timer():
    my_last_id = last_id;
    while True:
        current_num = last_id - my_last_id;
        response = urllib2.urlopen(url=watchUrl + str(current_num), timeout=10)  # 发送页面请求
        response.close()
        my_last_id = last_id;

        time.sleep(60 * 60 * 2)

def http_post(ts, level, issue):
    #post方式时候要发送的数据
    values=[
        {
            "timestamp": ts,
            "alarmlevel": level,
            "alarmissue": issue,
            "alarmsource": myTeam,
            "alarmadder": myReportor
        }
    ]

    jsData = json.dumps(values)                 # 对数据进行JSON格式化编码
    req = urllib2.Request(postUrl, jsData)      # 生成页面请求的完整数据
    response = urllib2.urlopen(req, timeout=10)             # 发送页面请求

    print response.read()                       # 获取服务器返回的页面信息
    response.close()

    return

def is_post_alarm(content) :
    for tag in postAlarms :
        if 0 < content.find(tag) :
            return True;

    return False

def my_connect():
    conn = MySQLdb.connect(host='10.100.156.208', port=3306, user='root', passwd='tc12345', charset="utf8", db='itchatdb')
    # conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='root', charset="utf8", db='itchatdb')
    conn.autocommit(1);
    return conn

def check_mydb():
    global mydb
    try:
        mydb.ping()
    except:
        mydb.close()
        mydb = my_connect()

############### Main ##########################################################
# 1. all receivers

# 2. mysql connected
mydb = my_connect()

# 3. startup post service
print 'startup post service'

last_id = 0;
cursor = mydb.cursor()
cursor.execute('select last_id from meta;');
row = cursor.fetchone();
if None == row :
    last_id = 0;
else:
    last_id = int(row[0]);

cursor.close();

last_id = last_id - 2;

thread.start_new_thread(hourly_timer, ())

while True :
    check_mydb()
    cursor = mydb.cursor()

    eSql = "select id, msg from message where id > %d limit 100;" % int(last_id);
    cursor.execute(eSql)

    results = cursor.fetchall();
    for row in results :
        print row[0], row[1];
        last_id = int(row[0]);

        if is_post_alarm(row[1]) :
            http_post(int(time.time()), myLevel, row[1])

    cursor.close();

    time.sleep(1)
