import configparser
from logg import logg
import os
import mysql.connector
import sys
from mysql.connector import errorcode

'''
生成注释文档需要
'''
__all__ = ('getConfig', 'getDbConnection', 'getDbTcpConnection', 'closeDBC', 'closeDbConnection', 'query', 'do')

def getConfig(sec,key):
    '''
    从配置文件中读取配置参数
    :param sec:
    :param key:
    :return:
    '''

    config = configparser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + '/db.conf'
    config.read(path)
    return config.get(sec,key)

def getDbTcpConnection(dbsec):
    '''
    以TCP的形式连接数据库，获得链接
    :param dbsec:
    :return:
    '''
    host = getConfig(dbsec,'host')
    dbname = getConfig(dbsec,'dbname')
    user = getConfig(dbsec,'user')
    passwd = getConfig(dbsec,'pass')


    try:
       con = mysql.connector.connect(host=host,database=dbname,user=user,password=passwd)  
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logg.e("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logg.e("Database does not exist")
        else:
            logg.e(err)
        sys.exit()
    return con


    
def getDbConnection(dbsec):
    host = getConfig(dbsec,'host')
    dbname = getConfig(dbsec,'dbname')
    user = getConfig(dbsec,'user')
    passwd = getConfig(dbsec,'pass')
    socket = getConfig(dbsec,'socket')

    logg.d('connect host %s, db %s, user %s' % (host,dbname,user))

    try:
       con = mysql.connector.connect(unix_socket=socket,database=dbname,user=user,password=passwd)  
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logg.e("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logg.e("Database does not exist")
        else:
            logg.e(err)
        sys.exit()
    return con

def closeDbConnection(con):
    '''
    Deprecated
    :param con:
    :return:
    '''
    con.close()

def closeDBC(con):
    con.close()

def query(con,sql):
    logg.d(sql)
    try:
        cur = con.cursor()
        
        cur.execute(sql)          
    except mysql.connector.Error as err:
        logg.e('sql execute error')
        return False
    return cur


def do(con,sql):
    try:
        cur = con.cursor()
        cur.execute(sql)          
        con.commit()
    except mysql.connector.Error as err:
        logg.e('sql execute error')
        return False
    return True 
