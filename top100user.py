import utils
import sys

class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

def process(table,limiter):
    sql = "select uid,sum(price) s from "+ table + " group by uid order by s desc limit %d " % limiter
    cur2 = utils.query(con2,sql)
    if cur2 == False:
        #in case of DB query error
        print('Table %s no data' % table)
        return
    for (uid,s) in cur2:
        s = int(s)
        uid = int(uid)
        uids[uid] = uids.get(uid,0) + s
    cur2.close()
   
# main 
#disable print buffer
sys.stdout = Unbuffered(sys.stdout)

#Build 2 Db connections
con=utils.getDbTcpConnection('log2017') 
con2=utils.getDbTcpConnection('log2017') 

#limiter specified the row number selected from one DB table
limiter = 10

#uid is a dictionary contains the user and the prices
uids = {}

#go through all target tables
sql = "show tables like 'log20170%_b'"

cur = utils.query(con,sql)
for (table,) in cur:
    process(table,limiter)
cur.close()

price_sorted = sorted(zip(uids.values(),uids.keys()) ,reverse = True)

for item in price_sorted:
  print('Value:%10d , Uid %12d' % item) 
utils.closeDbConnection(con)
utils.closeDbConnection(con2)
