import utils
import sys
import re
import calendar as cal
import datetime
import time
class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

def gettime(yearmon):
    year = int(yearmon[0:4])
    mon = int(yearmon[4:])
    d = cal.monthrange(year,mon)
    s1 = datetime.datetime(year,mon,1,0,0,0)
    s2 = datetime.datetime(year,mon,d[1],23,59,59)
    stime = time.mktime(s1.timetuple()) 
    etime = time.mktime(s2.timetuple()) 
    return stime,etime

def process(table):

    # get the suspect user 
    dotal = {}
    top = set()
    oddhour = set()
    all = set()
    oddday = set()

    sql = "select uid,sum(price) s from %s group by uid order by s desc limit 300 " % table
    print sql
    cur2 = utils.query(con2,sql)
    for (uid,s) in cur2:
        top.add(uid)
    cur2.close()

    sql = "select distinct uid from %s" % table
    print sql
    cur2 = utils.query(con2,sql)
    for (uid,) in cur2:
        all.add(uid)
    cur2.close()

    sql = "select uid,from_unixtime(time,'%Y%m%d %H') t,sum(price) s from "+ table + " group by uid,t having s > 100" 
    print sql
    cur2 = utils.query(con2,sql)
    for (uid,t,s) in cur2:
        oddhour.add(uid)
    cur2.close()

    sql = "select uid,from_unixtime(time,'%Y%m%d') t,sum(price) s from "+ table + " group by uid,t having s > 100" 
    print sql
    cur2 = utils.query(con2,sql)
    for (uid,t,s) in cur2:
        oddday.add(uid)
    cur2.close()
    
    topoddhour = top & oddhour
    topoddday = top & oddday
    print '=',table,len(all),len(oddday),len(oddhour),len(topoddhour),len(topoddday)
# main 
sys.stdout = Unbuffered(sys.stdout)
con=utils.getDbTcpConnection('it') 
con2=utils.getDbTcpConnection('it') 
if con == False: 
    print "DB con error"
    sys.exit()

sql = "show tables like 'log2017%_b'"
cur = utils.query(con,sql)
for (table,) in cur:
    process(table)
cur.close()
    
utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
