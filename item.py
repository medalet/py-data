import utils
import sys
import re
import calendar as cal
import datetime
import time
con=utils.getDbTcpConnection('localdb') 
con2=utils.getDbTcpConnection('localdb') 
if con == False: 
    print "DB con error"
    sys.exit()

sql = "show databases like 'pid%'"
cur = utils.query(con,sql)
for (database,) in cur:
    print database
    table = database + '.' + database + '_fee'
    sql = 'select price,sum(price),count(distinct uid),count(*) from %s group by price' % table
    cur2 = utils.query(con2,sql)
    if cur2 == False:
        continue
    for(item,total,users,nums) in cur2:
        print item,total,users,nums
    cur2.close()
cur.close()
    
utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
