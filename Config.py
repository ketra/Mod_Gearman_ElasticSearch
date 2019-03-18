import ConfigParser
import os
import sys

class Pyconfig:
    def __init__(self, pylogger):
        self._config = ConfigParser.ConfigParser()
        self._log = pylogger
        pylogger.debug('Getting Config')
        # path = os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), 'Config', 'Config.cfg')
        path = os.path.join('/etc/Elastic_Gearman', 'Config.cfg')
        self._config.read(path)
        self.Read()

    def Read(self):
        try:

            # toon Settings
            self.Elastic_Server = self._configvalueget('General', 'elastic_ip')
            self.GearmanIP = self._configvalueget('General', 'gearman_ip')
            self.Gearman_Queue = self._configvalueget('General', 'gearman_queue')
            self.Gearman_secret = self._configvalueget('General', 'secret')

            # Debug Setting.
            if False:
                self.DebugConfig()
            self._log.debug("Config Read.")
        except Exception as e:
            self._log.error(e)

    def DebugConfig(self):
        for attr in dir(self):
            if not attr.startswith("_") and not callable(getattr(self, attr)):
                self._log.debug("Config.%s = %s" % (attr, getattr(self, attr)))

    def _configvalueget(self, section, name):
        value = ''
        try:
            value = self._config.get(section, name)
        except Exception as e:
            self._log.error(e)
        finally:
            return value

    def _ConfigBooleanGet(self, section, name):
        value = False
        try:
            value = self._config.getboolean(section, name)
        except Exception as e:
            self._log.error(e)
        finally:
            return value

    def _ConfigIntGet(self, section, name):
        value = False
        try:
            value = self._config.getint(section, name)
        except Exception as e:
            self._log.error(e)
        finally:
            return value
