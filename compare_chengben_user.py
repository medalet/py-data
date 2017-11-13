import utils
import util_time
import sys
from collections import defaultdict

con_d=utils.getDbTcpConnection('d') 
con_cpc_2017=utils.getDbTcpConnection('cpc_2017') 
if con_d == False: 
    print("DB con error")
    sys.exit()
if con_cpc_2017 == False: 
    print("DB con error")
    sys.exit()

sql = 'select id as rid,gid,new_user as users,mon from cpc_chengben order by mon,rid,gid'
cur = utils.query(con_d,sql)
cpc_chengben = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for (rid,gid,users,mon) in cur:
    cpc_chengben[mon][rid][gid] = users

cpc_users = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for mon in range(1,11):
    first_timestamp,last_timestamp = util_time.get_month_timestamp(2017,mon)
    month = 1700+mon
    stime = int(first_timestamp)
    etime = int(last_timestamp) 
    sql = 'select rid,gid,count(uid) as users from u_2017 where time >= %d and time <= %d  group by rid,gid' % (stime,etime)
    #print(sql)
    cur = utils.query(con_cpc_2017,sql)
    for (rid,gid,users) in cur:
        cpc_users[month][rid][gid] = users

for mon,tdic in cpc_chengben.items():
    for rid,ttdic in tdic.items():
        for gid,users in ttdic.items():
            realusers = cpc_users[mon][rid][gid]
            if realusers == users:
                print('OK month=%d rid=%d gid=%d chengben=realuser=%d' % (mon,rid,gid,users))            
            else:
                print('ERR month=%d rid=%d gid=%d chengben=%d realuser=%d' % (mon,rid,gid,users,realusers))            


utils.closeDbConnection(con_d)
utils.closeDbConnection(con_cpc_2017)
sys.exit()
   
