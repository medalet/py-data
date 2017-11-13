import utils
import sys
import re
import calendar as cal
import datetime
import time

con2=utils.getDbTcpConnection('it') 
con=utils.getDbTcpConnection('it') 
if con2 == False: 
    print "DB con error"
    sys.exit()
#tables = ['105','107','108','109','201','202','203','204','510','702','707']
#tables = ['203','204','510','702','707']
tables = ['202']
for tt in tables:
    table = 'pid'+tt+'_fee'
    sql = ' select uid,sum(price) s from %s group by uid order by s desc limit 5000' % table
    print sql
    cur = utils.query(con,sql)
    for (uid,s) in cur:
        sql = "update "+ table + " set time = time+36000 where uid = '"+uid+"' and from_unixtime(time,'%H') in ('01','02','03','04','05','06') " 
        utils.query(con2,sql)
        con2.commit()
        
    cur.close()

    
utils.closeDbConnection(con2)
utils.closeDbConnection(con)


   
