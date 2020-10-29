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

import logging
import time

from ujson import dumps

from google.protobuf import json_format
from thingsboard_gateway.tb_client.proto.transport_pb2 import *
from thingsboard_gateway.tb_client.tb_device_mqtt import TBDeviceMqttClient
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

PROTO_REQUEST = "/req"
PROTO_RESPONSE = "/rsp"
PROTO_RPC = "/rpc"
PROTO_CONNECT = "/con"
PROTO_DISCONNECT = "/dis"
PROTO_TELEMETRY = "/tel"
PROTO_ATTRIBUTES = "/atr"
PROTO_CLAIM = "/clm"
PROTO_ACTION = "/act"
PROTO_ATTRIBUTES_RESPONSE = PROTO_ATTRIBUTES + PROTO_RESPONSE
PROTO_ATTRIBUTES_REQUEST = PROTO_ATTRIBUTES + PROTO_REQUEST
GATEWAY_MAIN_TOPIC = "v2/g"
GATEWAY_CONNECT_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_CONNECT
GATEWAY_DISCONNECT_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_DISCONNECT
GATEWAY_ATTRIBUTES_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_ATTRIBUTES
GATEWAY_TELEMETRY_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_TELEMETRY
# TODO Add Claim device processing
# GATEWAY_CLAIM_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_CLAIM
GATEWAY_RPC_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_RPC
GATEWAY_DEVICE_ACTION_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_ACTION
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_ATTRIBUTES_REQUEST
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = GATEWAY_MAIN_TOPIC + PROTO_ATTRIBUTES_RESPONSE

log = logging.getLogger("tb_connection")


class TBGatewayAPI:
    pass


class TBGatewayMqttClient(TBDeviceMqttClient):
    def __init__(self, host, port, token=None, gateway=None, quality_of_service=1):
        super().__init__(host, port, token, quality_of_service)
        self.quality_of_service = quality_of_service
        self.__max_sub_id = 0
        self.__sub_dict = {}
        self.__connected_devices = set("*")
        self.devices_actions_handler = None
        self.devices_server_side_rpc_request_handler = None
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client._on_unsubscribe = self._on_unsubscribe
        self._gw_subscriptions = {}
        self.gateway = gateway

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        super()._on_connect(client, userdata, flags, result_code, *extra_params)
        if result_code == 0:
            self._gw_subscriptions[
                int(self._client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, qos=1)[1])] = GATEWAY_ATTRIBUTES_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, qos=1)[
                                           1])] = GATEWAY_ATTRIBUTES_RESPONSE_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_RPC_TOPIC, qos=1)[1])] = GATEWAY_RPC_TOPIC
            # self._gw_subscriptions[int(self._client.subscribe(GATEWAY_RPC_RESPONSE_TOPIC)[1])] = GATEWAY_RPC_RESPONSE_TOPIC

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        subscription = self._gw_subscriptions.get(mid)
        if subscription is not None:
            if mid == 128:
                log.error("Service subscription to topic %s - failed.", subscription)
                del self._gw_subscriptions[mid]
            else:
                log.debug("Service subscription to topic %s - successfully completed.", subscription)
                del self._gw_subscriptions[mid]

    def _on_unsubscribe(self, *args):
        log.debug(args)

    def get_subscriptions_in_progress(self):
        return True if self._gw_subscriptions else False

    def _on_message(self, client, userdata, message):
        super()._on_decoded_message(message)
        self._on_decoded_message(message)

    def _on_decoded_message(self, message):
        if message.topic.startswith(GATEWAY_DEVICE_ACTION_TOPIC):
            content = self._convert_response_payload_to_proto_object(message, GatewayActionMsg)
            content = self._convert_to_json(content)
            if content["action"] == "DELETED":
                device_name = content["device"]
                self.__connected_devices.remove(device_name)
                log.info("Device %s was removed on ThingsBoard!")
            if self.devices_actions_handler is not None:
                self.devices_actions_handler(self, content)
        if message.topic.startswith(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC):
            content = self._convert_response_payload_to_proto_object(message, GatewayAttributeResponseMsg)
            content = self._convert_to_json(content)
            with self._lock:
                req_id = content["id"]
                # pop callback and use it
                if self._attr_request_dict[req_id]:
                    self._attr_request_dict.pop(req_id)(content, None)
                else:
                    log.error("Unable to find callback to process attributes response from TB")
        elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
            content = self._convert_response_payload_to_proto_object(message, GatewayAttributeUpdateNotificationMsg)
            content = self._convert_to_json(content)
            with self._lock:
                # callbacks for everything
                if self.__sub_dict.get("*|*"):
                    for callback in self.__sub_dict["*|*"]:
                        self.__sub_dict["*|*"][callback](content)
                # callbacks for device. in this case callback executes for all attributes in message
                target = content["device"] + "|*"
                if self.__sub_dict.get(target):
                    for callback in self.__sub_dict[target]:
                        self.__sub_dict[target][callback](content)
                # callback for atr. in this case callback executes for all attributes in message
                targets = [content["device"] + "|" + attribute for attribute in content["data"]]
                for target in targets:
                    if self.__sub_dict.get(target):
                        for sub_id in self.__sub_dict[target]:
                            self.__sub_dict[target][sub_id](content)
        elif message.topic == GATEWAY_RPC_TOPIC:
            content = self._convert_response_payload_to_proto_object(message, GatewayRpcResponseMsg)
            content = self._convert_to_json(content)
            if self.devices_server_side_rpc_request_handler:
                self.devices_server_side_rpc_request_handler(self, content)

    def __request_attributes(self, device, keys, callback, type_is_client=False):
        if not keys:
            log.error("There are no keys to request")
            return False
        ts_in_millis = int(round(time.time() * 1000))
        attr_request_number = self._add_attr_request_callback(callback)
        proto_msg = GatewayAttributesRequestMsg()
        proto_msg.deviceName = device
        proto_msg.client = type_is_client
        for key in keys:
            proto_msg.key.append(key)
        info = self._client.publish(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, proto_msg.SerializeToString(), 1)
        self._add_timeout(attr_request_number, ts_in_millis + 30000)
        return info

    def gw_request_shared_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, False)

    def gw_request_client_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, True)

    def gw_send_attributes(self, device, attributes, quality_of_service=1):
        proto_msg = GatewayAttributesMsg()
        attributes_msg = AttributesMsg()
        attributes_msg.deviceName = device
        self._convert_attributes_to_proto(attributes, attributes_msg.msg.kv)
        proto_msg.msg.append(attributes_msg)
        return self.publish_data(proto_msg.SerializeToString(), GATEWAY_ATTRIBUTES_TOPIC, quality_of_service)

    def gw_send_telemetry(self, device, telemetry, quality_of_service=1):
        if not isinstance(telemetry, list) and not (isinstance(telemetry, dict) and telemetry.get("ts") is not None):
            telemetry = [telemetry]
        proto_msg = GatewayTelemetryMsg()
        proto_msg.msg.append(self.__convert_telemetry_to_proto(telemetry, device))
        return self.publish_data(proto_msg.SerializeToString(), GATEWAY_TELEMETRY_TOPIC, quality_of_service)

    def gw_connect_device(self, device_name, device_type):
        proto_msg = ConnectMsg()
        proto_msg.deviceName = device_name
        proto_msg.deviceType = device_type
        info = self._client.publish(topic=GATEWAY_CONNECT_TOPIC,
                                    payload=proto_msg.SerializeToString(),
                                    qos=self.quality_of_service)
        self.__connected_devices.add(device_name)
        log.debug("Connected device %s", device_name)
        return info

    def gw_disconnect_device(self, device_name):
        proto_msg = DisconnectMsg()
        proto_msg.deviceName = device_name
        info = self._client.publish(topic=GATEWAY_DISCONNECT_TOPIC,
                                    payload=proto_msg.SerializeToString(),
                                    qos=self.quality_of_service)
        self.__connected_devices.remove(device_name)
        log.debug("Disconnected device %s", device_name)
        return info

    def gw_subscribe_to_all_attributes(self, callback):
        return self.gw_subscribe_to_attribute("*", "*", callback)

    def gw_subscribe_to_all_device_attributes(self, device, callback):
        return self.gw_subscribe_to_attribute(device, "*", callback)

    def gw_subscribe_to_attribute(self, device, attribute, callback):
        if device not in self.__connected_devices:
            log.error("Device %s is not connected", device)
            return False
        with self._lock:
            self.__max_sub_id += 1
            key = device + "|" + attribute
            if key not in self.__sub_dict:
                self.__sub_dict.update({key: {self.__max_sub_id: callback}})
            else:
                self.__sub_dict[key].update({self.__max_sub_id: callback})
            log.info("Subscribed to %s with id %i", key, self.__max_sub_id)
            return self.__max_sub_id

    def gw_unsubscribe(self, subscription_id):
        with self._lock:
            for attribute in self.__sub_dict:
                if self.__sub_dict[attribute].get(subscription_id):
                    del self.__sub_dict[attribute][subscription_id]
                    log.info("Unsubscribed from %s, subscription id %i", attribute, subscription_id)
            if subscription_id == '*':
                self.__sub_dict = {}

    def gw_set_server_side_rpc_request_handler(self, handler):
        self.devices_server_side_rpc_request_handler = handler

    def gw_send_rpc_reply(self, device, req_id, resp, quality_of_service):
        if quality_of_service is None:
            quality_of_service = self.quality_of_service
        if quality_of_service not in (0, 1):
            log.error("Quality of service (qos) value must be 0 or 1")
            return None
        proto_msg = GatewayRpcResponseMsg()
        proto_msg.deviceName = device
        proto_msg.id = req_id
        proto_msg.data = resp
        info = self._client.publish(GATEWAY_RPC_TOPIC,
                                    proto_msg.SerializeToString(),
                                    qos=quality_of_service)
        return info

    def __convert_telemetry_to_proto(self, telemetry, device):
        telemetry_msg = TelemetryMsg()
        telemetry_msg.deviceName = device
        ts_kv_list_proto = super()._convert_telemetry_to_proto(telemetry)
        telemetry_msg.msg.tsKvList.append(ts_kv_list_proto)
        return telemetry_msg

    def _convert_to_json(self, proto_message):
        result = super()._convert_to_json(proto_message)
        converted_message = json_format.MessageToDict(proto_message, True, True)
        if not result:
            if isinstance(proto_message, GatewayAttributeResponseMsg):
                result["device"] = converted_message["deviceName"]
                types = ['clientAttributeList', 'sharedAttributeList']
                correct_mapping = {'clientAttributeList': 'client', 'sharedAttributeList': 'shared'}
                result.update(**self._get_correct_key_value_by_attribute_type(converted_message, types, correct_mapping))
            elif isinstance(proto_message, GatewayAttributeUpdateNotificationMsg):
                result["device"] = converted_message["deviceName"]
                types = ['sharedUpdated', 'sharedDeleted']
                result.update(**self._get_correct_key_value_by_attribute_type(converted_message["notificationMsg"], types, {"sharedUpdated": "shared"})["shared"])
            elif isinstance(proto_message, GatewayRpcResponseMsg):
                result["device"] = converted_message["deviceName"]
                result["id"] = converted_message["id"]
                result["data"] = converted_message["data"]
            elif isinstance(proto_message, GatewayActionMsg):
                result["device"] = converted_message["msg"]["deviceName"]
                result["action"] = converted_message["msg"]["action"]
        return result
