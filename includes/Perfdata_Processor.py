"""
Project name : Mod_Gearman_ElasticSearch
Author : Perfdata_Processor.
Last edited : 17-3-2019
Version : 
"""
import re

# UNIT_REGEX = re.compile('([0-9.]+)([^0-9.]+)?')
UNIT_REGEX = r"([^\s]+|'[^']+')=([-.\d]+)(c|s|ms|us|B|KB|MB|GB|TB|%)?" r"(?:;([-.\d]+))?(?:;([-.\d]+))?(?:;([-.\d]+))?(?:;([-.\d]+))?"


class Perfdata_Processor:
    def __init__(self):
        pass

    @staticmethod
    def normalize_to_unit(value, unit):
        """Normalize the value to the unit returned.
        We use base-1000 for second-based units, and base-1024 for
        byte-based units. Sadly, the Nagios-Plugins specification doesn't
        disambiguate base-1000 (KB) and base-1024 (KiB).
        """
        if unit == 'ms':
            return value / 1000.0
        if unit == 'us':
            return value / 1000000.0
        if unit == 'KB':
            return value * 1024
        if unit == 'MB':
            return value * 1024 * 1024
        if unit == 'GB':
            return value * 1024 * 1024 * 1024
        if unit == 'TB':
            return value * 1024 * 1024 * 1024 * 1024

    @staticmethod
    def parse_perfdata(s):
        """Parse performance data from a perfdata string
        """
        metrics = []
        counters = re.findall(UNIT_REGEX, s)

        for (key, value, uom, warn, crit, min, max) in counters:
            try:
                norm_value = Perfdata_Processor.normalize_to_unit(float(value), uom)
                metrics.append((key, norm_value))
            except ValueError:
                pass
            #    self.log.warning(
            #        "Couldn't convert value '{value}' to float".format(
            #            value=value))

        return metrics

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
