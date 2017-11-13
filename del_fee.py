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

sql = "show tables like 'pid0_fee'"
cur = utils.query(con,sql)
for (table,) in cur:
    sql = 'delete from %s where  mod(conv(right(uid,10),16,10)+time,3) = 1' % table
    print sql
    utils.query(con2,sql)
cur.close()
    
utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
