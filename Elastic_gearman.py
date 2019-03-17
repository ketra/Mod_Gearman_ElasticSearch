#!/usr/bin/env python
import sys

from Config import Pyconfig
from Logger.Logging import PyLogger
from includes.Gearman_Worker import Gearman_Worker
from includes.daemon import Daemon


class ElasticGearman(Daemon):

    def run(self):
        print('Running...')
        self.logger = PyLogger('Elastic_gearman').logger
        cfg = Pyconfig(self.logger)
        gearman = Gearman_Worker(self.logger,
                                 cfg.GearmanIP,
                                 cfg.Gearman_Queue,
                                 cfg.Gearman_secret,
                                 cfg.Elastic_Server)
        gearman.StartWorker()


if __name__ == "__main__":
    daemon = ElasticGearman('/omd/sites/Monitoring/var/run/ElasticGearman.pid',
                            '/omd/sites/Monitoring/var/log/ElasticGearman-console.log',
                            '/omd/sites/Monitoring/var/log/ElasticGearman-console.log',
                            '/omd/sites/Monitoring/var/log/ElasticGearman-console.log')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
