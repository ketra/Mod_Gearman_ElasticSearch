#!/usr/bin/env python
import gearman
import base64
from Crypto.Cipher import AES
from elasticsearch import Elasticsearch
import datetime

###### Change these settings
GearmanIP = ['0.0.0.0:4730']
ElasticSearchIP = ['http://0.0.0.0']
Secret = 'ChangeMe'
GearmanQueue = 'elastic'
##### Dont edit below this line;

gm_worker = gearman.GearmanWorker(GearmanIP)
es = Elasticsearch(ElasticSearchIP)


def _extract_fields(line):
    """Extract the key/value fields from a line of performance gearman data
    """
    acc = {}
    field_tokens = line.split("\t")
    for field_token in field_tokens:
        kv_tokens = field_token.split('::')
        if len(kv_tokens) == 2:
            (key, value) = kv_tokens
            acc[key] = value

    return acc

def _extract_perdata(line):
    """Extract Key/Value fields for the performance data"""
    perfdata = {}
    tokens = line.split(' ')
    for token in tokens:
        kv_tokens = token.split('=')
        if len(kv_tokens) == 2:
            (key, value) = kv_tokens
            perfdata[key] = value
    return perfdata

def insert_service_data(data):
    """insert PerformanceData into Elasticsearch"""
    doc = {'server': data['hostname'], 'service': data['service'], 'timestamp': data['timestamp'], 'metric': {'name' : data['label'], 'data': data['perfdata']} }
    res = es.index(index="service_perfdata" + datetime.datetime.now().strftime("%Y%m"), body=doc, doc_type='doc')
    print(res)

# See gearman/job.py to see attributes on the GearmanJob
# Send back a reversed version of the 'data' string through WORK_DATA instead of WORK_COMPLETE
def task_listener_reverse_inflight(gearman_worker, gearman_job):
    try:
        cipher = AES.new(Secret,AES.MODE_ECB)
        decrypted = cipher.decrypt(base64.b64decode(gearman_job.data))
        formatted = _extract_fields(decrypted)
#       print(formatted)
        if formatted['DATATYPE'] == 'SERVICEPERFDATA':
            perfdata = _extract_perdata(formatted['SERVICEPERFDATA'])
            print('host: {host}, service {service} has performancedata {perfdata}'.format(host=formatted['HOSTNAME'],service=formatted['SERVICEDESC'],perfdata=perfdata))
            for value in perfdata:
                data = {}
                data['timestamp'] = formatted['TIMET']
                data['hostname'] = formatted['HOSTNAME']
                data['service'] = formatted['SERVICEDESC']
                data['label'] = value
                data['perfdata'] = perfdata[value]
                insert_service_data(data)
        else:
            perfdata = _extract_perdata(formatted['HOSTPERFDATA'])
            print('host: {host}, has performancedata {perfdata}'.format(host=formatted['HOSTNAME'],perfdata=perfdata))
        return(decrypted)
    except Exception as e:
        print(e)
        return ""


# gm_worker.set_client_id is optional
gm_worker.register_task(GearmanQueue, task_listener_reverse_inflight)

# Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
gm_worker.work()
