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
tables = ['106','508']
for table in tables:
    table = 'pid'+table+'_fee'
    sql = 'update %s,(select uid u,sum(price) s from %s group by u order by s desc limit 107,10000 ) as t set %s.time = %s.time - round(rand()*3600*5) where %s.uid = t.u' % (table,table,table,table,table)
    utils.query(con2,sql)
    con2.commit()

utils.closeDbConnection(con2)


   
