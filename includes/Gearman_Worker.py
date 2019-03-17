"""
Project name : Mod_Gearman_ElasticSearch
Author : Gearman_Worker.
Last edited : 17-3-2019
Version :
"""
import base64

import gearman
from Crypto.Cipher import AES

from Elastic import ElasticWorker
from Perfdata_Processor import Perfdata_Processor


class Gearman_Worker:

    def __init__(self, gearman_ip, gearman_queue):
        self.gm_worker = gearman.GearmanWorker(gearman_ip)
        self.secret = 'vQsCyDObFsYSGeh1oZ41Br6JSO5V6VaN'
        self.elastic = ElasticWorker('http://192.168.2.120')
        self.gm_worker.register_task(gearman_queue, self.task_listener_reverse_inflight)
        pass

    # See gearman/job.py to see attributes on the GearmanJob
    # Send back a reversed version of the 'data' string through WORK_DATA instead of WORK_COMPLETE
    def task_listener_reverse_inflight(self, gearman_worker, gearman_job):
        try:
            cipher = AES.new(self.secret,
                             AES.MODE_ECB)
            decrypted = cipher.decrypt(base64.b64decode(gearman_job.data))
            formatted = Perfdata_Processor.extract_fields(decrypted)
            #       print(formatted)
            if formatted['DATATYPE'] == 'SERVICEPERFDATA':
                perfdata = Perfdata_Processor.parse_perfdata(formatted['SERVICEPERFDATA'])
                print(
                    'host: {host}, service {service} has performancedata {perfdata}'.format(host=formatted['HOSTNAME'],
                                                                                            service=formatted[
                                                                                                'SERVICEDESC'],
                                                                                            perfdata=perfdata))
                for (key, value) in perfdata:
                    data = dict()
                    data['timestamp'] = formatted['TIMET']
                    data['hostname'] = formatted['HOSTNAME']
                    data['service'] = formatted['SERVICEDESC']
                    data['label'] = key
                    data['perfdata'] = perfdata[value]
                    self.elastic.insert_service_data(data)
            else:
                perf_data = Perfdata_Processor.parse_perfdata(formatted['HOSTPERFDATA'])
                print('host: {host}, has performance data {perf_data}'.format(
                    host=formatted['HOSTNAME'],
                    perf_data=perf_data))
            return decrypted
        except Exception as e:
            print(e)
            return ""

    def StartWorker(self):
        self.gm_worker.work()
