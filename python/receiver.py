# -*- coding: utf-8 -*-
#!/usr/bin/env python
import os
import sys
import requests
import itchat
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf8')

from itchat.content import *

transAlarms = [
    # from docker's alarms
    'Fail to clear log',
	'Process abnormally',
	'Can\'t find available port',
	'Service dead',
    # from dbrouter's alarms
    'EXECING_SLOW_SQL',
	'CONFIG_ERROR',
    'DB_CONN_BREAK',
	'DB_ACCOUNT',
	'DB_QUERY_FAIL',
	'DB_OTHER_ERROR',
    'SQL_TIMEOUT',
    'SQL_LONG_TIMEOUT',
    'GLOBAL_ID',
]

def extract_msg(content, left, right, def_msg) :
    start = content.find(left)
    if start < 0:
        return def_msg

    end = content.find(right)
    if end <= 0 or start >= end:
        return def_msg

    return content[start + len(left):end]

def get_brief(content) :
    return extract_msg(content, u'<des><![CDATA[', u']]></des>', 'Text')

def get_report_msg(url) :
    req = requests.get(url)
    if None == req :
        return url

    return extract_msg(req.text, u'报警内容：', u'</body>', url)

def need_transfer_alarm(content) :
    for tag in transAlarms :
        if 0 < content.find(tag) :
            return True;

    return False

def my_connect():
    # conn = MySQLdb.connect(host='10.100.156.208', port=3306, user='root', passwd='tc12345', charset="utf8", db='itchatdb')
    conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='root', charset="utf8", db='itchatdb')
    if None != conn:
        conn.autocommit(1)

    return conn

def check_mydb():
    global mydb
    try:
        mydb.ping()
    except:
        mydb = my_connect()

# register Sharing msg callback function
@itchat.msg_register([SHARING, TEXT], isMpChat=True)
def text_tranfer(msg):
    #show_msg(msg)

    # Be not from 'tongchengbaojing'
    if mp_ur != msg['FromUserName'] :
        return

    # can not need transfer alarm
    if not need_transfer_alarm(msg['Content']) :
        return

    # get brief from Content
    # brief = get_report_msg(msg['Url'])
    brief = get_brief(msg['Content'])


    sql = "replace into message(msg) values('%s');" % (brief.replace("'", "''"))

    # itchat.send('%s\n%s' % (mp_nm, brief), toUserName=chatroom_id)
    try :
        cursor = mydb.cursor()
        cursor.execute(sql)
        cursor.close()
    except:
        check_mydb()
        cursor = mydb.cursor()
        cursor.execute(sql)
        cursor.close()

############### Main ##########################################################
# 0. loging
itchat.auto_login(hotReload=True, enableCmdQR=2)
itchat.auto_login(hotReload=True)
# itchat.login()

# 1. get target massive platforms(public)
list = itchat.get_mps(update=True)
for map in list :
    # print map['PYQuanPin']
    # print map['NickName']
    # if u'xiaobing' == map['PYQuanPin'] :
    if u'tongchengbaojing' == map['PYQuanPin'] :
        mp_ur = map['UserName']
        mp_id = map['PYQuanPin']
        mp_nm = map['NickName']
        print mp_id, mp_nm, mp_ur
        break

# mysql connected
status = False;
mydb = my_connect();
status = True;

# 2. get target chatroom for transfer
# print '\nchatrooms .>>>>>'
list = itchat.get_chatrooms(update=True, contactOnly=True)
for chatroom in list :
    if 'tongchengshujuzhongjianjian' == chatroom['PYQuanPin'] :
        chatroom_id = chatroom['UserName']
        # realReceivers.append(chatroom_id)
        break

# 3. startup itchat
print 'Ready'
itchat.run(False)



