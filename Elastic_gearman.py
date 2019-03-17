#!/usr/bin/env python
import sys

from includes.Gearman_Worker import Gearman_Worker
from includes.daemon import Daemon

# Change these settings
GearmanIP = ['192.168.2.120:4730']
GearmanQueue = 'elastic'
# Dont edit below this line;


class ElasticGearman(Daemon):

    def run(self):
        print('Running...')
        gearman = Gearman_Worker(GearmanIP, GearmanQueue)
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
