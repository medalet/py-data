import utils
import sys
import re
import calendar as cal
import datetime
import time

con2=utils.getDbTcpConnection('it') 
if con2 == False: 
    print "DB con error"
    sys.exit()
tables = ['105','107','108','109','201','202','203','204','510','702','707']
for table in tables:
    table = 'pid'+table+'_fee'
    sql = 'update %s,(select floor(time/3600) m,uid u,sum(price) s from %s group by m,u having s > 150) as t set %s.time = %s.time - round(rand()*3600*72) where %s.uid = t.u' % (table,table,table,table,table)
    print sql
    utils.query(con2,sql)
    con2.commit()

    sql = 'update %s,(select floor(time/3600) m,uid u,sum(price) s from %s group by m,u having s > 100) as t set %s.time = %s.time - round(rand()*3600*36) where %s.uid = t.u' % (table,table,table,table,table)
    print sql
    utils.query(con2,sql)
    con2.commit()
    sql = 'update %s,(select floor(time/3600) m,uid u,sum(price) s from %s group by m,u having s > 70) as t set %s.time = %s.time - round(rand()*3600*10) where %s.uid = t.u' % (table,table,table,table,table)
    print sql
    utils.query(con2,sql)
    con2.commit()
    
utils.closeDbConnection(con2)


   
