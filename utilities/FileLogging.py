import logging
from logging.handlers import TimedRotatingFileHandler

class FileLogging():
    def __init__(self,job_name,log_level,path):
        self.logger=logging.getLogger(job_name)
        self.log_level=log_level
        self.path=path

    def create_timed_rotating_log(self):
        self.logger.setLevel('{0}'.format(self.log_level))
        try:

            handler = TimedRotatingFileHandler(self.path,
                                            when="m", #when to rotate m => minute
                                            interval=60, # after how many minutes => 1
                                            backupCount=5) # how many days of log files to keep => 10


            formatter = logging.Formatter('%(asctime)s -%(name)s- %(levelname)s - [%(filename)s:%(lineno)s:%(funcName)20s() ]- %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.info("===================Rotating log initiated=====================")

        except Exception as e:
            self.logger.error("=================Could not create timed rotating log. \n" + "{0}==========================".format(e))
            raise