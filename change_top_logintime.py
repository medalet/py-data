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
tables = ['510','702','707']
for tt in tables:
    table = 'pid'+tt+'_fee'
    sql = ' select uid,min(time) t, op,sum(price) s from %s group by uid order by s desc limit 1000' % table
    print sql
    cur = utils.query(con,sql)
    for (uid,t,op,s) in cur:
        it = int(t)
        x = time.localtime(int(t)) 
        yearmon = time.strftime('%Y%m',x)
        ntable = 'pid'+tt+'_action_'+yearmon
        sql = "update %s set time = %d-round(rand()*600) where uid = '%s' order by id limit 1" % (ntable,t,uid)
        utils.query(con2,sql)
        con2.commit()
        
    cur.close()

    
utils.closeDbConnection(con2)
utils.closeDbConnection(con)


   
