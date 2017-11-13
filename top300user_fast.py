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

def getMinIndex(my_list):
    min = my_list[0]
    for i in my_list:
        if i < min:
            min = i
    return my_list.index(min)

def process(table):

    # get the suspect user 
    top = set()
    oddhour = set()
    all = set()
    oddday = set()

    uids = {}
    sql = "select uid,from_unixtime(time,'%Y%m%d%H') t,sum(price) s from "+ table + " group by uid,t " 
    print sql
    cur2 = utils.query(con2,sql)
    for (uid,t,s) in cur2:
        s = int(s)
        if s>100:
            oddhour.add(uid)

        day = t[0:8]
        if uids.has_key(uid):
            h = uids[uid]
            if h.has_key(day):
                h[day] += s
            else:
                h[day] = s
        else:
            h ={day:s}
            uids[uid] = h
    cur2.close()
   
    topuser = [0]
    topuserv = [0]
    for uid,h in uids.iteritems():
        all.add(uid)
        ss = 0
        for day,s in h.iteritems():
            if s > 100:
                oddday.add(uid)
            ss +=s       

        if len(topuser) <300:
            min = topuserv[-1]
            if ss > min:
                minuid = topuser.pop()
                topuser.append(uid)
                topuser.append(minuid)

                min = topuserv.pop()
                topuserv.append(ss)
                topuserv.append(min)
            else:
                topuser.append(uid)
                topuserv.append(ss)
        else:
	    i = getMinIndex(topuserv) 
            min = topuserv[i]
            if ss > min:
                topuser[i] = uid
                topuserv[i] = ss
            
    top = set(topuser)                
    
    topoddhour = top & oddhour
    topoddday = top & oddday
    print '=',table,len(all),len(oddday),len(oddhour),len(topoddhour),len(topoddday)
    print oddday

# main 
sys.stdout = Unbuffered(sys.stdout)
con=utils.getDbTcpConnection('it') 
con2=utils.getDbTcpConnection('it') 
if con == False: 
    print "DB con error"
    sys.exit()

process('pid0_fee')

"""
sql = "show tables like 'pid5%_fee'"
cur = utils.query(con,sql)
for (table,) in cur:
    process(table)
cur.close()


sql = "show tables like 'pid7%_fee'"
cur = utils.query(con,sql)
for (table,) in cur:
    process(table)
cur.close()
"""
    
utils.closeDbConnection(con)
utils.closeDbConnection(con2)


   
