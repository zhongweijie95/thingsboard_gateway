#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from re import search
from time import time, sleep

from ujson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

from queue import Queue, Empty, Full
from threading import Thread


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config, connector_convert_callback):
        self.__config = config.get('converter')
        self.__connector_convert_callback = connector_convert_callback
        self.__processing_queue = Queue(self.__config.get("converterMaxQueueSize", 10000000))
        self.__converter_threads = {}
        self.__stopped = False
        self.__converter_workers = self.__config.get("converterWorkersCount", 1)
        for thread_number in range(self.__converter_workers):
            thread = Thread(daemon=True, name="Converting thread %i" % thread_number, target=self.__converting_threads_main)
            self.__converter_threads[thread_number] = thread
            thread.start()
        log.info("%i converter workers started.", self.__converter_workers)

    def convert(self, config, data):
        try:
            self.__processing_queue.put((config, data), True)
        except Full:
            log.error("maxConverterQueueSize (%i) is reached, cannot add message from topic %s to queue!", self.__processing_queue.maxsize, config)

    def __converting_threads_main(self):
        while not self.__stopped:
            if not self.__processing_queue.empty():
                try:
                    if self.__stopped:
                        break
                    current_record = self.__processing_queue.get(False)
                    result = self.__convert(*current_record)
                    self.__connector_convert_callback(result, current_record[0])
                except Empty:
                    if self.__stopped:
                        break
            else:
                if self.__stopped:
                    break
                sleep(.01)

    def stop(self):
        self.__stopped = True

    def __convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), data, expression_instead_none=True)
            elif self.__config.get("deviceNameTopicExpression") is not None:
                search_result = search(self.__config["deviceNameTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceName"] = search_result.group(0)
                else:
                    log.debug("Regular expression result is None. deviceNameTopicExpression parameter will be interpreted as a deviceName\n Topic: %s\nRegex: %s", config, self.__config.get("deviceNameTopicExpression"))
                    dict_result["deviceName"] = self.__config.get("deviceNameTopicExpression")
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeJsonExpression") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), data, expression_instead_none=True)
            elif self.__config.get("deviceTypeTopicExpression") is not None:
                search_result = search(self.__config["deviceTypeTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceType"] = search_result.group(0)
                else:
                    log.debug("Regular expression result is None. deviceTypeTopicExpression will be interpreted as a deviceType\n Topic: %s\nRegex: %s", config, self.__config.get("deviceTypeTopicExpression"))
                    dict_result["deviceType"] = self.__config.get("deviceTypeTopicExpression")
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    value = TBUtility.get_value(datatype_config["value"], data, datatype_config["type"], expression_instead_none=True)
                    value_tag = TBUtility.get_value(datatype_config["value"], data, datatype_config["type"], get_tag=True)
                    key = TBUtility.get_value(datatype_config["key"], data, datatype_config["type"], expression_instead_none=True)
                    key_tag = TBUtility.get_value(datatype_config["key"], data, get_tag=True)
                    if ("${" not in str(value) and "}" not in str(value)) \
                       and ("${" not in str(key) and "}" not in str(key)):
                        is_valid_key = isinstance(key, str) and "${" in datatype_config["key"] and "}" in datatype_config["key"]
                        is_valid_value = isinstance(value, str) and "${" in datatype_config["value"] and "}" in datatype_config["value"]
                        full_key = datatype_config["key"].replace('${' + str(key_tag) + '}', str(key)) if is_valid_key else key_tag
                        full_value = datatype_config["value"].replace('${' + str(value_tag) + '}', str(value)) if is_valid_value else value
                        if datatype == 'timeseries' and (data.get("ts") is not None or data.get("timestamp") is not None):
                            dict_result[datatypes[datatype]].append({"ts": data.get('ts', data.get('timestamp', int(time()))), 'values': {full_key: full_value}})
                        else:
                            dict_result[datatypes[datatype]].append({full_key: full_value})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)
        return dict_result
