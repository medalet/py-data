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

#sql = 'select id as rid,gid,new_user as users,mon from cpc_chengben order by mon,rid,gid'
sql = 'select id as rid,gid,new_user as users,mon from cpc_chengben where mon = 1710 order by mon,rid,gid'
cur = utils.query(con_d,sql)
cpc_chengben = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for (rid,gid,users,mon) in cur:
    cpc_chengben[mon][rid][gid] = users

for mon,tdic in cpc_chengben.items():
    for rid,ttdic in tdic.items():
        for gid,users in ttdic.items():
            month = mon % 100
            first_timestamp,last_timestamp = util_time.get_month_timestamp(2017,month)
            stime = int(first_timestamp)
            etime = int(last_timestamp)
            sql = 'select uid from uu_2017 where rid = %d and gid = %d and time >= %d and time <= %d order by uid limit %d,1' % (rid,gid,stime,etime,users)
            #print(sql)
            cur = utils.query(con_cpc_2017,sql)
            for (uid,) in cur:
                #print("mon = %d,rid = %d,gid = %d,users = %d,uid = %d" % (mon,rid,gid,users,uid))
                msql = 'delete from uu_2017 where rid = %d and gid = %d and time >= %d and time <= %d and uid >= %s' % (rid,gid,stime,etime,uid)
                print(msql)
                utils.do(con_cpc_2017,msql)
                break
            else:
                sql = 'select count(uid) as nu from uu_2017 where rid = %d and gid = %d and time >= %d and time <= %d ' % (rid,gid,stime,etime)
                cur = utils.query(con_cpc_2017,sql)
                for (nu,) in cur:
                    print("mon = %d,rid = %d,gid = %d,users = %d,real_user = %d" % (mon,rid,gid,users,nu))
        

utils.closeDbConnection(con_d)
utils.closeDbConnection(con_cpc_2017)
