import logging
import logging.config

def init():
    logging.config.fileConfig('./conf/logging.conf')

class logg:
    def __init__(self):
        logger = logging.getLogger('self')
        logger.setLevel(logging.INFO) 

    def d(self,str):
        logger.debug(str)

    def i(self,str):
        logger.info(str)

    def e(self,str):
        logger.error(str)

    def w(self,str):
        logger.warning(str)
