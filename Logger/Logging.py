import ConfigParser
import logging
import logging.config
import logging.handlers
import os
import sys

from EnhancedRotatingFileHandler import EnhancedRotatingFileHandler


class PyLogger:
    """
    The Pylogger Class
    """

    def __init__(self, name):
        """
        This initialises The Logger
        @param name: This name will be the name of the loggfile
        """
        self.logger = logging.getLogger(name)
        self.name = name
        self.location = r'{name}.log'.format(name=name)
        self.loggingdays = 14
        self.level = 'INFO'
        self._configLogger()

    def _configLogger(self):
        """
        This will configure the logger.
        @return: None
        """
        self._config()
        self.logger.setLevel(self.level)
        if self.level == 'DEBUG':
            FORMAT = '%(asctime)s - Line:%(lineno)-3s %(module)-18s - %(funcName)-20s - [%(levelname)-6s] - %(message)s'
        else:
            FORMAT = '%(asctime)s - %(funcName)10s - %(module)10s - %(levelname)5s - %(message)s'
        formatter = logging.Formatter(FORMAT)
        if not (os.path.exists(os.path.dirname(self.location))):
            os.makedirs(os.path.dirname(self.location))
        # file = logging.handlers.TimedRotatingFileHandler(self.location, when='M', interval=1, backupCount=self.loggingdays, utc=False)
        # file = logging.handlers.RotatingFileHandler(self.location, maxBytes=10*1024*1024, backupCount=self.loggingdays)
        file = EnhancedRotatingFileHandler(os.path.join(self.location, self.name + '.log'), when='midnight', interval=1,
                                           backupCount=self.loggingdays, maxBytes=15 * 1024 * 1024)
        stream = logging.StreamHandler()
        file.setFormatter(formatter)
        stream.setFormatter(formatter)
        self.logger.addHandler(file)
        self.logger.addHandler(stream)

    def Write(self, Message):
        self.logger.error(Message)

    def _config(self):
        """
        This function helps collect the config from the config file.
        @return: None
        """
        try:
            config = ConfigParser.ConfigParser()
            path = os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), 'Config', 'Config.cfg')
            # path = os.path.join('/etc/TOON2SQL', 'Config.cfg')
            print(sys.argv[0])
            config.read(path)
            self.location = config.get('Logging', 'Location')
            self.loggingdays = config.getint('Logging', 'Days')
            self.level = config.get('Logging', 'Level')
        except Exception as e:
            print(e)
