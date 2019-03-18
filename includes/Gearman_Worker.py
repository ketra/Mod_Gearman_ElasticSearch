"""
Project name : Mod_Gearman_ElasticSearch
Author : Gearman_Worker.
Last edited : 17-3-2019
Version :
"""
import base64
import traceback
import gearman
from Crypto.Cipher import AES

from Elastic import ElasticWorker
from Perfdata_Processor import Perfdata_Processor


class Gearman_Worker:

    def __init__(self, logger, gearman_ip, gearman_queue, secret, elastic):
        self.logger = logger
        self.logger.debug('IP = ' + gearman_ip)
        self.gm_worker = gearman.GearmanWorker([gearman_ip])
        self.secret = secret
        self.elastic = ElasticWorker(self.logger, elastic)
        self.gm_worker.register_task(gearman_queue, self.task_listener_reverse_inflight)
        pass

    # See gearman/job.py to see attributes on the GearmanJob
    # Send back a reversed version of the 'data' string through WORK_DATA instead of WORK_COMPLETE
    def task_listener_reverse_inflight(self, gearman_worker, gearman_job):
        try:
            cipher = AES.new(self.secret,
                             AES.MODE_ECB)
            decrypted = cipher.decrypt(base64.b64decode(gearman_job.data))
            self.logger.debug(decrypted)
            formatted = Perfdata_Processor.extract_fields(decrypted)
            self.logger.debug(formatted)
            #       print(formatted)
            if formatted['DATATYPE'] == 'SERVICEPERFDATA':
                perfdata = Perfdata_Processor.parse_perfdata(formatted['SERVICEPERFDATA'])
                self.logger.debug('host: {host}, service {service} '.format(host=formatted['HOSTNAME'], service=formatted['SERVICEDESC']))
                for (key, value) in perfdata:
                    data = dict()
                    data['timestamp'] = formatted['TIMET']
                    data['hostname'] = formatted['HOSTNAME']
                    data['service'] = formatted['SERVICEDESC']
                    data['label'] = key
                    data['perfdata'] = value
                    self.elastic.insert_service_data(data)
            else:
                perf_data = Perfdata_Processor.parse_perfdata(formatted['HOSTPERFDATA'])
                self.logger.debug('host: {host}, has performance data'.format(
                    host=formatted['HOSTNAME']))
                perfdata = Perfdata_Processor.parse_perfdata(formatted['HOSTPERFDATA'])
                for (key, value) in perfdata:
                    data = dict()
                    data['timestamp'] = formatted['TIMET']
                    data['hostname'] = formatted['HOSTNAME']
                    data['label'] = key
                    data['perfdata'] = value
                    self.elastic.insert_host_data(data)
            return decrypted
        except Exception as e:
            self.logger.exception(e)
            return ""

    def StartWorker(self):
        self.gm_worker.work()
