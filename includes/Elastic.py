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
        self.es = Elasticsearch(elastic_search_ip)
        pass

    def insert_service_data(self, data):
        """insert PerformanceData into Elasticsearch"""
        value, unit = Perfdata_Processor.parse_value_and_unit(data['perfdata'])
        doc = {
            'server': data['hostname'],
            'service': data['service'],
            'timestamp': data['timestamp'],
            'metric': {'name': data['label'],
                       'data': value, "unit": unit}
        }
        res = self.es.index(
            index="service_perfdata_" + datetime.datetime.now().strftime("%Y_%m"),
            body=doc,
            doc_type='_doc')
        # print(res)
