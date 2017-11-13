import utils
import sys
import re

con=utils.getDbConnection('localdb') 
if con == False: 
    print "DB con error"
    sys.exit()

top = {}
for year in [2013,2014,2015]:
  for mon in range(1,13):
    print "process mon %02d" % mon
    sql = 'select gid,uid,sum(price) t from log%d%02d_a  group by gid,uid having t > 10000 ' % (year,mon)
    print sql
    cur = utils.query(con,sql)
    if cur == False:
        print "DB query error: %s"  % sql
        continue
    for (gid,uid,t) in cur:
        if top.has_key(gid):
            if top[gid].has_key(uid):
                top[gid][uid] += t
            else:
                top[gid][uid] = t
        else:
            top[gid] = {uid:t}
    cur.close()    

for gid in top:
    #sorted(top[gid].iteritems(),key=lambda d:d[1],reverse=True)
    for uid in top[gid]:
        print gid,uid,top[gid][uid]  
utils.closeDbConnection(con)


  
