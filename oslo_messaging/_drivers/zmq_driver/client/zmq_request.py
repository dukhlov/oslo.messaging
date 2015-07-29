#    Copyright 2015 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import logging

import six

from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class Request(object):

    def __init__(self, target, context=None, message=None, retry=None):

        if self.msg_type not in zmq_names.MESSAGE_TYPES:
            raise RuntimeError("Unknown message type!")

        self.target = target
        self.context = context
        self.message = message
        self.retry = retry

    @abc.abstractproperty
    def msg_type(self):
        """ZMQ message type"""

    def close(self):
        """Nothing to close in base request"""


class RpcRequest(Request):

    def __init__(self, *args, **kwargs):
        message = kwargs.get("message")
        if message['method'] is None:
            errmsg = _LE("No method specified for RPC call")
            LOG.error(errmsg)
            raise KeyError(errmsg)

        self.timeout = kwargs.pop("timeout")
        assert self.timeout is not None, "Timeout should be specified!"

        super(RpcRequest, self).__init__(*args, **kwargs)


class CallRequest(RpcRequest):

    msg_type = zmq_names.CALL_TYPE

    def __init__(self, *args, **kwargs):
        self.allowed_remote_exmods = kwargs.pop("allowed_remote_exmods")
        super(CallRequest, self).__init__(*args, **kwargs)


class CastRequest(RpcRequest):

    msg_type = zmq_names.CAST_TYPE


class FanoutRequest(RpcRequest):

    msg_type = zmq_names.CAST_FANOUT_TYPE


class NotificationRequest(Request):

    msg_type = zmq_names.NOTIFY_TYPE

    def __init__(self, *args, **kwargs):
        self.version = kwargs.pop("version")
        super(NotificationRequest, self).__init__(*args, **kwargs)


class NotificationFanoutRequest(NotificationRequest):

    msg_type = zmq_names.NOTIFY_FANOUT_TYPE
