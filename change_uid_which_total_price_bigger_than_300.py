import utils
import sys
import logging
import time


class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


def get_uid_from_table_by_limiter(table, limiter):
    sql = "select uid,sum(price) totalprice from " + table + " group by uid having totalprice > %d " % limiter
    logging.debug('Sql: %s' % sql)
    cur2 = utils.query(con2, sql)
    logging.debug('Sql OK: %s' % sql)
    if cur2 == False:
        # in case of DB query error
        logging.info('Table %s no qualified row' % table)
        return
    row = 0
    for (uid, totalprice) in cur2:
        row = row + 1
        print('Process row %d' % row, end='\r')
        totalprice = int(totalprice)
        uid = int(uid)
        uids[uid] = uids.get(uid, 0) + totalprice
    cur2.close()


def update_specified_uid(uids, tables):
    pass


# main
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s  %(levelname)s %(message)s')

logging.debug('Disable the print buffer')
sys.stdout = Unbuffered(sys.stdout)

# Build 2 Db connections
logging.debug('Starting to build the mysql connection')
con = utils.getDbTcpConnection('log2017')
con2 = utils.getDbTcpConnection('log2017')
logging.debug('The mysql connection OK')

'''
limiter specified the row number selected from one DB table
there are 10 tables(log201701-log201710) .
In order to fetch the users which year price is bigger than 300, you should fetch users which month price is bigger than 300/10, and put them together. It avoid a entire user scan.
'''
top_price = 3000 * 100
logging.debug('Total price limiter = %d cents' % limiter)
limiter = top_price / 10
logging.debug('Setting the table limiter = %d' % limiter)

# uid is a dictionary contains the user and the prices
uids = {}
tables = []

# go through all target tables
sql = "show tables like 'log20170%_b'"
logging.debug('Sql: %s' % sql)

cur = utils.query(con, sql)
for (table,) in cur:
    tables.append(table)
    logging.debug('Starting to processing table: %s, limiter: %d' % (table, limiter))
    get_uid_from_table_by_limiter(table, limiter)
cur.close()

uids_final = {key: value for key, value in uids.items() if value > top_price}
price_sorted = sorted(zip(uids_final.values(), uids_final.keys()), reverse=True)
for item in price_sorted:
    logging.info('Value:%10d , Uid %12d' % item)

logging.info('There are %d users which year price is bigger than %d' % (len(uids_final), top_price))
update_specified_uid(uids_final, tables)

utils.closeDbConnection(con)
utils.closeDbConnection(con2)
