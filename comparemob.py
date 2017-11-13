import utils
import sys
import re

con=utils.getDbConnection('localdb') 
if con == False: 
    print "DB con error"
    sys.exit()

conit=utils.getDbTcpConnection('it') 
if conit == False: 
    print "DB con error"
    sys.exit()


mobit = []
mob = []

sql = 'select distinct uid as uid from log201304_b  where op = 1 and type in (1,2,4)' 
cur = utils.query(conit,sql)
if cur == False:
    print "DB query error: %s"  % sql
else:
    for (uid,) in cur:
        mobit.append(uid)
    cur.close()    

sql = 'select distinct uid as uid from log201304  where op = 1 and type in (1,2,4)' 
cur = utils.query(con,sql)
if cur == False:
    print "DB query error: %s"  % sql
else:
    for (uid,) in cur:
        mob.append(uid)
    cur.close()    

print len(set(mobit).intersection(set(mob)))

utils.closeDbConnection(con)
utils.closeDbConnection(conit)


   
