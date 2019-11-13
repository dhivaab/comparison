import logging
import os
import config

def enablelog():
    logging.basicConfig(filename=os.getcwd() + '/' + config.logpath, filemode="w", format='%(asctime)s %(message)s')  
    logger=logging.getLogger() 
    logger.setLevel(logging.INFO) 
    return logger