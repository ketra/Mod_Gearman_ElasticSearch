"""
Project name : Mod_Gearman_ElasticSearch
Author : Elastic.
Last edited : 17-3-2019
Version : 
"""
import datetime

from elasticsearch import Elasticsearch

from Perfdata_Processor import Perfdata_Processor


class ElasticWorker:

    def __init__(self, logger, elastic_search_ip):
        self.logger = logger
        self.logger.debug('elasticsearch ip = {ip}'.format(ip=elastic_search_ip))
        self.es = Elasticsearch(elastic_search_ip)
        pass

    def insert_service_data(self, data):
        """insert PerformanceData into Elasticsearch"""
        value = data['perfdata']
        doc = {
            'server': data['hostname'],
            'service': data['service'],
            'timestamp': data['timestamp'],
            'metric': {'name': data['label'],
                       'data': value}
        }
        res = self.es.index(
            index="service_perfdata_" + datetime.datetime.now().strftime("%Y_%m"),
            body=doc,
            doc_type='_doc')
        self.logger.debug(res['result'])

    def insert_host_data(self, data):
        """insert PerformanceData into Elasticsearch"""
        value = data['perfdata']
        doc = {
            'server': data['hostname'],
            'timestamp': data['timestamp'],
            'metric': {'name': data['label'],
                       'data': value}
        }
        res = self.es.index(
            index="host_perfdata_" + datetime.datetime.now().strftime("%Y_%m"),
            body=doc,
            doc_type='_doc')
        self.logger.debug(res['result'])
