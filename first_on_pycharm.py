import utils
import sys

con = utils.getDbTcpConnection('log2017')
if con == False:
    print
    "DB con error"
    sys.exit()

sql = 'show tables'

cur = utils.query(con,sql)

for (table,) in cur:
    print(table)
cur.close()

utils.closeDbConnection(con)
