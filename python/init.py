#-*-coding:utf-8-*-
import sys, os, time, MySQLdb

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
"""
以下内容根据用户需求设定，目前支持（hash/range）
"""

dbSize = 2      # DB数量
tableSize = 2   # Sharding table数量

action = "CREATE" # CREATE: 执行建表建库, DROP: 执行删表删库
mysqlNode = [     # 真实的db实例列表
	{'host': '10.100.156.208', 'port': 3301, 'user': 'root', 'pasd': 'tc12345'},
	{'host': '10.100.156.208', 'port': 3302, 'user': 'root', 'pasd': 'tc12345'},
	# {'host': '10.100.156.208', 'port': 3306, 'user': 'root', 'pasd': 'tc12345'},
	# {'host': '10.100.156.208', 'port': 3307, 'user': 'root', 'pasd': 'tc12345'},
	# {'host': '10.100.47.3', 'port': 3000, 'user': 'root', 'pasd': '!p@ssword123'},
	# {'host': '10.100.47.4', 'port': 3000, 'user': 'root', 'pasd': '!p@ssword123'},
]

oriDbName = 'wxhdb'             # 逻辑DB名称
oriTableName = 't_hash'  # 逻辑Table名称

# 原始的建表SQL
tableSql = """
CREATE TABLE IF NOT EXISTS `t_hash` (
  `uid` bigint NOT NULL,
  `data` int,
  `s1` varchar(100) not null,
  `s2` varchar(100) not null,
  `CreateTime` datetime NOT NULL,
  `UpdateTime` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='销售单详情';
"""

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
"""
以下内容是固定配置
"""

dbNameFormat = '%s_%d'       # DB分库的名称格式
tableNameFormat = '%s_s_%d'  # Table分表的名称格式

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
"""
执行逻辑
"""
ignoreAll = False
mysqlConn = {}

def getConn(node):
	global mysqlConn
	nodeKey = "%s:%d" % (node['host'], node['port'])
	if mysqlConn.has_key(nodeKey):
		return mysqlConn[nodeKey]
	
	db = MySQLdb.connect(host=node['host'], port=node['port'], user=node['user'], passwd=node['pasd'], charset="utf8")
	db.autocommit(1)
	mysqlConn[nodeKey] = db
	return db

def confirm(info):
	global ignoreAll
	input = raw_input(info)
	if input == 'Y' or input == 'y':
		return True
	elif input == 'A' or input == 'a':
		ignoreAll = True
		return True
	return False

def getDBName(idx):
	if dbSize != tableSize:
		if dbSize == len(mysqlNode):
			dbName = oriDbName
		else:
			dbPerNode = dbSize / len(mysqlNode)
			if dbPerNode * len(mysqlNode) != dbSize:
				print "Error dbSize=%d, mysqlNodeSize=%d, exit" % (dbSize, len(mysqlNode))
				sys.exit()
			tablePerDb = tableSize / dbSize
			if tablePerDb * dbSize != tableSize:
				print "Error dbSize=%d, tableSize=%d, exit" % (dbSize, tableSize)
				sys.exit()
			idx = idx / tablePerDb
			idx = idx % dbPerNode
			dbName = dbNameFormat % (oriDbName, idx)
	else:
		dbName = dbNameFormat % (oriDbName, idx)
	return dbName

def getTableName(idx):
	if dbSize != tableSize:
		tablePerDb = tableSize / dbSize
		if tablePerDb * dbSize != tableSize:
			print "Error dbSize=%d, tableSize=%d, exit" % (dbSize, tableSize)
			sys.exit()
		idx = idx % tablePerDb
	tableName = tableNameFormat % (oriTableName, idx)
	return tableName

def getMysqlNode(idx):
	nodeSize = len(mysqlNode)
	idx = idx * nodeSize / tableSize
	return mysqlNode[idx]

def needCreateDB(idx):
	dbIdx = idx * dbSize / tableSize
	if (dbIdx * tableSize / dbSize) == idx:
		return True
	return False

def createDB(idx):
	node = getMysqlNode(idx)
	conn = getConn(node)
	dbName = getDBName(idx)
	
	try:
		cursor = conn.cursor()
		
		# check if exist
		sql = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s'" % (dbName, )
		cursor.execute(sql)
		data = cursor.fetchone()
		if data:
			if not ignoreAll and not confirm('DB(%s) exist, continue(Y) or exit(N) or ignroe all(A)? ' % dbName):
				print 'Stop create, exit'
				sys.exit()
			else:
				cursor.close()
				return
		
		# create db
		print "Create DB %s" % (dbName, )
		sql = "CREATE DATABASE `%s` /*!40100 DEFAULT CHARACTER SET utf8 */" % (dbName, )
		cursor.execute(sql)
		
		cursor.close()
	except Exception, e:
		print 'Error: occur exception on create db(%s), node: %s, exit' % (dbName, node)
		print 'Exception: ', e
		sys.exit()
	return

def createTable(idx):
	node = getMysqlNode(idx)
	conn = getConn(node)
	dbName = getDBName(idx)
	tableName = getTableName(idx)
	
	try:
		cursor = conn.cursor()
		
		# check if exist
		sql = " SELECT * FROM information_schema.`TABLES`  where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbName, tableName)
		cursor.execute(sql)
		data = cursor.fetchone()
		if data:
			if not ignoreAll and not confirm('Table(%s.%s) exist, continue(Y) or exit(N) or ignroe all(A)? ' % (dbName, tableName)):
				print 'Stop create, exit'
				sys.exit()
			else:
				cursor.close()
				return
		
		# create table
		print "Create table %s.%s" % (dbName, tableName)
		if tableSql.find('`%s`' % oriTableName) >= 0:
			sql = tableSql.replace(oriTableName, '%s`.`%s' % (dbName, tableName), 1)
		else:
			sql = tableSql.replace(oriTableName, '%s.%s' % (dbName, tableName), 1)
		cursor.execute(sql)
		
		cursor.close()
	except Exception, e:
		print 'Error: occur exception on create table(%s.%s), node: %s, exit' % (dbName, tableName, node)
		print 'Exception: ', e
		sys.exit()
	return

def execCreate():
	for i in range(tableSize):
		if needCreateDB(i):
			createDB(i)
		createTable(i)
	return

def canDropDB(idx):
	dbIdx = idx * dbSize / tableSize + 1
	if (dbIdx * tableSize / dbSize - 1) == idx:
		return True
	return False

def dropDB(idx):
	node = getMysqlNode(idx)
	conn = getConn(node)
	dbName = getDBName(idx)
	
	try:
		cursor = conn.cursor()
		
		# drop db
		print "Drop DB %s" % (dbName, )
		sql = "DROP DATABASE `%s`" % (dbName, )
		cursor.execute(sql)
		
		cursor.close()
	except Exception, e:
		print 'Error: occur exception on drop db(%s), node: %s, exit' % (dbName, node)
		sys.exit()
	return

def dropTable(idx):
	node = getMysqlNode(idx)
	conn = getConn(node)
	dbName = getDBName(idx)
	tableName = getTableName(idx)
	
	try:
		cursor = conn.cursor()
		
		# drop table
		print "Drop table %s.%s" % (dbName, tableName)
		sql = "DROP TABLE `%s`.`%s`" % (dbName, tableName)
		cursor.execute(sql)
		
		cursor.close()
	except Exception, e:
		print 'Error: occur exception on drop table(%s.%s), node: %s, exit' % (dbName, tableName, node)
		sys.exit()
	return

def execDrop():
	for i in range(tableSize):
		dropTable(i)
		if canDropDB(i):
			dropDB(i)
	return

def checkConfig():
	nodeSize = len(mysqlNode)
	
	if tableSize % dbSize != 0:
		print "Error: Table can't map to db, %d -> %d" % (tableSize, dbSize)
		sys.exit()
	
	if dbSize % nodeSize != 0:
		print "Error: DB can't map to node, %d -> %d" % (dbSize, nodeSize)
		sys.exit()
	
	return True

def main():
	checkConfig()
	
	if action == "CREATE" or action == "create":
		execCreate()
	elif action == "DROP" or action == "drop":
		execDrop()
	else:
		print "Error: action=" , action
	return

if __name__ == "__main__":

	if len(sys.argv) > 1:
		action = sys.argv[1];
	
	main()
	print "over"
