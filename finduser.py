import utils
import sys
import re
import calendar as cal
import datetime
import time

def gettime(yearmon):
    year = int(yearmon[0:4])
    mon = int(yearmon[4:])
    d = cal.monthrange(year,mon)
    s1 = datetime.datetime(year,mon,1,0,0,0)
    s2 = datetime.datetime(year,mon,d[1],23,59,59)
    stime = time.mktime(s1.timetuple()) 
    etime = time.mktime(s2.timetuple()) 
    return stime,etime

def process(uuid,yearmon,gid):
    stime,etime = gettime(yearmon)

    # get the suspect user 
    dotal = {}
    sql = "select uid,time,price from pid_act_fee.pid%s_fee  where uid = '%s' and time >= %d and time < %d " % (gid,uuid,stime,etime)
    cur = utils.query(con,sql)
    for (uuid,utime,price) in cur:
        stime1 = utime
        etime1 = utime+60*10

        d = set()
        logsql = 'select uid,sum(price)/100 t from d.log%s_b where time >%d and time < %d group by uid' % (yearmon,stime1,etime1)
        cur2 = utils.query(con2,logsql)
        for (uid,tt) in cur2:
            t = int(tt)
            if t>price*0.7 :
	        if(total.has_key(uid)):
                    total[uid] += 1
                else:
                    total[uid] = 1
        cur2.close()    
    cur.close()    
   
    s = 0 
    t = 0
    for k,v in total.iteritems():
        if v > s:
            t = k
            s = v;
    return t
 
con=utils.getDbTcpConnection('it') 
con2=utils.getDbTcpConnection('it') 
if con == False: 
    print "DB con error"
    sys.exit()

with open(r'suser.txt') as filehandle:
    for line in filehandle:
        line = line.strip()
        m = re.match('^(\w{32})\W(201[3,4,5]\d{2})\W(\d{3})$',line)
        if m:
            uuid = m.group(1)
            yearmon = m.group(2)
            gid = m.group(3)
            suser = process(uuid,yearmon,gid)
            print uuid,suser
        else:
            print 'format error',line

utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
