"""
Project name : Mod_Gearman_ElasticSearch
Author : Perfdata_Processor.
Last edited : 17-3-2019
Version : 
"""
import re

UNIT_REGEX = re.compile('([0-9.]+)([^0-9.]+)?')


class Perfdata_Processor:
    def __init__(self):
        pass

    @staticmethod
    def extract_fields(line):
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

    @staticmethod
    def extract_perdata(line):
        """Extract Key/Value fields for the performance data"""
        perf_data = {}
        tokens = line.split(' ')
        for token in tokens:
            kv_tokens = token.split('=')
            if len(kv_tokens) == 2:
                (key, value) = kv_tokens
                perf_data[key] = value
        return perf_data

    @staticmethod
    def parse_value_and_unit(raw_value):
        """Returns the value, unit tuple. Unit may be empty."""
        m = UNIT_REGEX.match(raw_value)
        if not m:
            return raw_value, ''
        value, unit = m.groups('')
        return value, unit
