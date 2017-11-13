import configparser
import os
import mysql.connector
import sys
from mysql.connector import errorcode

def getConfig(sec,key):
    config = configparser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + '/db.conf'
    config.read(path)
    return config.get(sec,key)

def getDbTcpConnection(dbsec):
    host = getConfig(dbsec,'host')
    dbname = getConfig(dbsec,'dbname')
    user = getConfig(dbsec,'user')
    passwd = getConfig(dbsec,'pass')


    try:
       con = mysql.connector.connect(host=host,database=dbname,user=user,password=passwd)  
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        sys.exit()
    return con


    
def getDbConnection(dbsec):
    host = getConfig(dbsec,'host')
    dbname = getConfig(dbsec,'dbname')
    user = getConfig(dbsec,'user')
    passwd = getConfig(dbsec,'pass')
    socket = getConfig(dbsec,'socket')


    try:
       con = mysql.connector.connect(unix_socket=socket,database=dbname,user=user,password=passwd)  
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        sys.exit()
    return con

def closeDbConnection(con):
    con.close()

def query(con,sql):
    try:
        cur = con.cursor()
        
        cur.execute(sql)          
    except mysql.connector.Error as err:
        print('sql execute error')
        return False
    return cur


def do(con,sql):
    try:
        cur = con.cursor()
        cur.execute(sql)          
        con.commit()
    except mysql.connector.Error as err:
        print('sql execute error')
        return False
    return True 
