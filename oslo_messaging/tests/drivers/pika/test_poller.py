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

import unittest
import time

from mock import mock

from oslo_messaging._drivers.pika_driver.pika_poller import PikaPoller


class PikaPollerTestCase(unittest.TestCase):
    def setUp(self):
        self._pika_engine = mock.Mock()
        self._poller_connection_mock = mock.Mock()
        self._poller_channel_mock = mock.Mock()
        self._poller_connection_mock.channel.return_value = (
            self._poller_channel_mock
        )
        self._pika_engine.create_connection.return_value = (
            self._poller_connection_mock
        )
        self._prefetch_count = 123

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = PikaPoller(self._pika_engine, self._prefetch_count,
                            incoming_message_class=incoming_message_class_mock)
        unused = object()
        method = object()
        properties = object()
        body = object()

        self._poller_connection_mock.process_data_events.side_effect = (
            lambda time_limit: poller._on_message_with_ack_callback(
                unused, method, properties, body
            )
        )

        poller.start()
        res = poller.poll()

        self.assertEqual(len(res), 1)

        self.assertEqual(res[0], incoming_message_class_mock.return_value)
        incoming_message_class_mock.assert_called_once_with(
            self._pika_engine, self._poller_channel_mock, method, properties,
            body
        )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_after_stop(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = PikaPoller(self._pika_engine, self._prefetch_count,
                            incoming_message_class=incoming_message_class_mock)

        n = 10
        params = []

        for i in xrange(n):
            params.append((object(), object(), object(), object()))

        index = [0]

        def f(time_limit):
            for i in xrange(10):
                poller._on_message_no_ack_callback(
                    *params[index[0]]
                )
                index[0] += 1

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=1)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], incoming_message_class_mock.return_value)
        self.assertEqual(
            incoming_message_class_mock.call_args_list[0][0],
            (self._pika_engine, None) + params[0][1:]
        )

        poller.stop()

        res2 = poller.poll(prefetch_size=n)

        self.assertEqual(len(res2), n-1)
        self.assertEqual(incoming_message_class_mock.call_count, n)

        self.assertEqual(
            self._poller_connection_mock.process_data_events.call_count, 1)

        for i in xrange(n-1):
            self.assertEqual(res2[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i+1][0],
                (self._pika_engine, None) + params[i+1][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_batch(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = PikaPoller(self._pika_engine, self._prefetch_count,
                            incoming_message_class=incoming_message_class_mock)

        n = 10
        params = []

        for i in xrange(n):
            params.append((object(), object(), object(), object()))

        index = [0]

        def f(time_limit):
            poller._on_message_with_ack_callback(
                *params[index[0]]
            )
            index[0] += 1

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=n)

        self.assertEqual(len(res), n)
        self.assertEqual(incoming_message_class_mock.call_count, n)

        for i in xrange(n):
            self.assertEqual(res[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i][0],
                (self._pika_engine, self._poller_channel_mock) + params[i][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)

    @mock.patch("oslo_messaging._drivers.pika_driver.pika_poller.PikaPoller."
                "_declare_queue_binding")
    def test_poll_batch_with_timeout(self, declare_queue_binding_mock):
        incoming_message_class_mock = mock.Mock()
        poller = PikaPoller(self._pika_engine, self._prefetch_count,
                            incoming_message_class=incoming_message_class_mock)

        n = 10
        timeout = 1
        sleep_time = 0.2
        params = []

        success_count = 5

        for i in xrange(n):
            params.append((object(), object(), object(), object()))

        index = [0]

        def f(time_limit):
            time.sleep(sleep_time)
            poller._on_message_with_ack_callback(
                *params[index[0]]
            )
            index[0] += 1

        self._poller_connection_mock.process_data_events.side_effect = f

        poller.start()
        res = poller.poll(prefetch_size=n, timeout=timeout)

        self.assertEqual(len(res), success_count)
        self.assertEqual(incoming_message_class_mock.call_count, success_count)

        for i in xrange(success_count):
            self.assertEqual(res[i], incoming_message_class_mock.return_value)
            self.assertEqual(
                incoming_message_class_mock.call_args_list[i][0],
                (self._pika_engine, self._poller_channel_mock) + params[i][1:]
            )

        self.assertTrue(self._pika_engine.create_connection.called)
        self.assertTrue(self._poller_connection_mock.channel.called)

        self.assertTrue(declare_queue_binding_mock.called)
