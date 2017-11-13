import utils
import sys
import re

def mob2gate(mob):
    pass
    return 999

def getmob(uid,list):
    mlen = len(list)
    if mlen == 0:
        return uid;
    new_mob = list[int(uid%mlen)]
    del list[int(uid%mlen)]
    return new_mob 

def getseed(con,table):
    l = [] 
    sql = "select mob from %s" % table
    cur = utils.query(con,sql)
    if cur == False:
        print "DB query error: %s"  % sql
    for (mob,) in cur:
        l.append(mob)
    cur.close()
    return l 

con=utils.getDbConnection('localdb') 
if con == False: 
    print "DB con error"
    sys.exit()

skefu = tuple(getseed(con,"kefu"))
sceshi = tuple(getseed(con,"ceshi")) 

for mon in range(1,10):
    kefu = list(skefu);
    ceshi = list(sceshi);
    table = "log2013%02d" % mon
    sql = 'select uid,sum(price) t from %s_a  where type =1 or type = 2 or type = 4  group by uid having t > 20000 order by t desc' % table 
    cur = utils.query(con,sql)
    if cur == False:
        print "DB query error: %s"  % sql
        continue
    for (uid,t) in cur:
    #    print uid,get_mob(uid) 
        if re.match('1\d{10}',str(uid)) == None:
            continue
        if t > 70000:
        #ceshi
            new_mob = getmob(uid,ceshi)
        else:
        #kefu
            new_mob = getmob(uid,kefu)
        print "update %s_b set uid = %d where uid = %d;" %(table,new_mob,uid)
    cur.close()    
utils.closeDbConnection(con)


   
