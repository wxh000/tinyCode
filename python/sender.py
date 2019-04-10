#-*-coding:utf-8-*-

#!/usr/bin/env python
import sys
import itchat
import MySQLdb
import time
import thread

reload(sys)
sys.setdefaultencoding('utf8')

receivers = [
    # u'walker',
    u'AK_liang',
    u'火车人thomas',
    # u'沐一',
    # u'刘海荣',
]

zmj = u'周小沫'

lhr = u'刘海荣'
alarm = u'alarm'

myAlarms = [
    # from docker's alarms
	'Process abnormally',
	'Service dead',
    # from dbrouter's alarms
    'EXECING_SLOW_SQL',
    'CONFIG_ERROR',
    'DB_CONN_BREAK',
	'DB_ACCOUNT',
	'DB_QUERY_FAIL',
	'DB_OTHER_ERROR',
#     'SQL_TIMEOUT',
#     'SQL_LONG_TIMEOUT',
    'GLOBAL_ID',
]

secondAlarms = [
    # from docker's alarms
	'Process abnormally',
	'Service dead',
    # from dbrouter's alarms
    'EXECING_SLOW_SQL',
    'CONFIG_ERROR',
    'DB_CONN_BREAK',
	'DB_ACCOUNT',
	'DB_QUERY_FAIL',
	'DB_OTHER_ERROR',
#     'SQL_TIMEOUT',
#     'SQL_LONG_TIMEOUT',
    'GLOBAL_ID',
]

# send msg to all recievers
def send_msg(msg):
    # send brief to chatroom & filehelper
    for usr in realReceivers :
        itchat.send('%s\n%s' % ('itchat', msg), toUserName=usr)

def is_report_monthly():
    current = time.localtime()

    if current.tm_mday in [1,2] and current.tm_hour == 9:
        return True

    if current.tm_mon in [1,3,5,7,8,10,12] and current.tm_mday == 31 and current.tm_hour == 9:
        return True

    if current.tm_mon in [4,6,9,10] and current.tm_mday == 30 and current.tm_hour == 9:
        return True

    if current.tm_mon == 2 and current.tm_mday == 28 and current.tm_hour == 9:
        return True

    return False

def is_report_giveback_money():
    current = time.localtime()

    if current.tm_mday in [19, 20, 21]:
        return True

    return False

# send weekly
def weekly_timer():
    while True:
        current = time.localtime();
        if current.tm_wday >= 4 and (current.tm_hour == 18):
            itchat.send("itchat alarm weekly report.", toUserName='filehelper')
            itchat.send("itchat alarm weekly report.", toUserName=chatroom_id)

        if is_report_monthly() :
            itchat.send("itchat alarm monthly report.", toUserName=chatroom_id)
            itchat.send("itchat alarm weekly report.", toUserName='filehelper')

        if is_report_giveback_money() and (current.tm_hour == 12):
            itchat.send("robot give back money, monthly.", toUserName='filehelper')
            itchat.send("robot give back money, monthly.", toUserName=zmjuser)


        time.sleep(60 * 20)

def is_second_alarm(content) :
    for tag in secondAlarms :
        if 0 < content.find(tag) :
            return True;

    return False

def is_my_alarm(content) :
    for tag in myAlarms :
        if 0 < content.find(tag) :
            return True;

    return False

def my_connect():
    conn = MySQLdb.connect(host='10.100.156.208', port=3306, user='root', passwd='tc12345', charset="utf8", db='itchatdb')
    # conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='root', charset="utf8", db='itchatdb')
    if None != conn:
        conn.autocommit(1)

    return conn

def check_mydb():
    global mydb
    try:
        mydb.ping()
    except:
        mydb = my_connect()

############### Main ##########################################################
# 0. loging
itchat.auto_login(True, enableCmdQR=0)
# itchat.auto_login(True, enableCmdQR=2)

# 1. all receivers
realReceivers = []
for usr in receivers:
    users = itchat.search_friends(name=usr)
    if len(users) > 0 :
        realReceivers.append(users[0]['UserName'])
        itchat.send('itchat Ready.', users[0]['UserName'])
#realReceivers.append('filehelper')

lhrusers = itchat.search_friends(name=lhr)
if len(lhrusers) :
    lhruser = lhrusers[0]['UserName']
    print 'lhr', lhruser

zmjUsers = itchat.search_friends(name=zmj)
if len(zmjUsers) :
    zmjuser = zmjUsers[0]['UserName']
    print 'zmj', zmjuser

chatrooms = itchat.get_chatrooms(update=True, contactOnly=True)
for chatroom in chatrooms:
    if 'tongchengshujuzhongjianjian' == chatroom['PYQuanPin']:
        chatroom_id = chatroom['UserName']
        break

# 2. mysql connected
mydb = my_connect()

# 3. startup itchat
print 'itchat Ready'

cursor = mydb.cursor()
cursor.execute('select last_id from meta;');
last_id = cursor.fetchone();
if None == last_id :
    last_id = 0;
cursor.close();

thread.start_new_thread(weekly_timer, ())

while True :
    check_mydb()
    cursor = mydb.cursor()
    cursor.execute("select id, msg from message where id > '%d' limit 100;" % (last_id))

    results = cursor.fetchall();
    for row in results :
        print row[0], row[1];
        last_id = row[0];
        for r in realReceivers :
            if is_my_alarm(row[1]) :
                r = itchat.send(row[1], toUserName=r)
                print r;

        if is_second_alarm(row[1]) :
            r = itchat.send(row[1], toUserName=lhruser)
            print r;


    cursor.execute("replace into meta(type, last_id) values(1, '%d')" % last_id);
    cursor.close();
    time.sleep(1)


