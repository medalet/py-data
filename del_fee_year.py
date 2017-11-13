import utils
import sys
import re
import calendar as cal
import datetime
import time
con=utils.getDbTcpConnection('it') 
con2=utils.getDbTcpConnection('it') 
if con == False: 
    print "DB con error"
    sys.exit()

sql = "show tables like 'pid%_fee_2%'"
cur = utils.query(con,sql)
for (table,) in cur:
    sql = 'drop table %s' % table
    print sql
    utils.query(con2,sql)
cur.close()
    
utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
