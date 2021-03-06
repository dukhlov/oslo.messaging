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

import fixtures
import testtools

import oslo_messaging
from oslo_messaging._drivers import impl_zmq
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_socket
from oslo_messaging.tests.drivers.zmq import zmq_common
from oslo_messaging.tests import utils as test_utils


zmq = zmq_async.import_zmq()


class ZmqTestPortsRange(zmq_common.ZmqBaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(ZmqTestPortsRange, self).setUp()

        # Set config values
        kwargs = {'rpc_zmq_min_port': 5555,
                  'rpc_zmq_max_port': 5560}
        self.config(**kwargs)

    def test_ports_range(self):
        listeners = []

        for i in range(10):
            try:
                target = oslo_messaging.Target(topic='testtopic_' + str(i))
                new_listener = self.driver.listen(target)
                listeners.append(new_listener)
            except zmq_socket.ZmqPortRangeExceededException:
                pass

        self.assertLessEqual(len(listeners), 5)

        for l in listeners:
            l.cleanup()


class TestConfZmqDriverLoad(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestConfZmqDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'zmq'

    def test_driver_load(self):
        transport = oslo_messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, impl_zmq.ZmqDriver)


class TestZmqBasics(zmq_common.ZmqBaseTestCase):

    def test_send_receive_raises(self):
        """Call() without method."""
        target = oslo_messaging.Target(topic='testtopic')
        self.listener.listen(target)
        self.assertRaises(
            KeyError,
            self.driver.send,
            target, {}, {'tx_id': 1},
            wait_for_reply=True,
            timeout=60)

    def test_send_receive_topic(self):
        """Call() with topic."""

        target = oslo_messaging.Target(topic='testtopic')
        self.listener.listen(target)
        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=True,
            timeout=60)
        self.assertTrue(result)

    def test_send_noreply(self):
        """Cast() with topic."""

        target = oslo_messaging.Target(topic='testtopic', server="my@server")
        self.listener.listen(target)
        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=False)

        self.listener._received.wait()

        self.assertIsNone(result)
        self.assertTrue(self.listener._received.isSet())
        method = self.listener.message.message[u'method']
        self.assertEqual(u'hello-world', method)

    def test_send_fanout(self):
        target = oslo_messaging.Target(topic='testtopic', fanout=True)

        self.listener.listen(target)

        result = self.driver.send(
            target, {},
            {'method': 'hello-world', 'tx_id': 1},
            wait_for_reply=False)

        self.listener._received.wait()

        self.assertIsNone(result)
        self.assertTrue(self.listener._received.isSet())
        method = self.listener.message.message[u'method']
        self.assertEqual(u'hello-world', method)

    def test_send_receive_direct(self):
        """Call() without topic."""

        target = oslo_messaging.Target(server='127.0.0.1')
        self.listener.listen(target)
        message = {'method': 'hello-world', 'tx_id': 1}
        context = {}
        result = self.driver.send(target, context, message,
                                  wait_for_reply=True,
                                  timeout=60)
        self.assertTrue(result)

    def test_send_receive_notification(self):
        """Notify() test"""

        target = oslo_messaging.Target(topic='t1',
                                       server='notification@server')
        self.listener.listen_notifications([(target, 'info')])

        message = {'method': 'hello-world', 'tx_id': 1}
        context = {}
        target.topic += '.info'
        self.driver.send_notification(target, context, message, '3.0')
        self.listener._received.wait(5)
        self.assertTrue(self.listener._received.isSet())


class TestPoller(test_utils.BaseTestCase):

    @testtools.skipIf(zmq is None, "zmq not available")
    def setUp(self):
        super(TestPoller, self).setUp()
        self.poller = zmq_async.get_poller()
        self.ctx = zmq.Context()
        self.internal_ipc_dir = self.useFixture(fixtures.TempDir()).path
        self.ADDR_REQ = "ipc://%s/request1" % self.internal_ipc_dir

    def test_poll_blocking(self):

        rep = self.ctx.socket(zmq.REP)
        rep.bind(self.ADDR_REQ)

        reply_poller = zmq_async.get_reply_poller()
        reply_poller.register(rep)

        def listener():
            incoming, socket = reply_poller.poll()
            self.assertEqual(b'Hello', incoming[0])
            socket.send_string('Reply')
            reply_poller.resume_polling(socket)

        executor = zmq_async.get_executor(listener)
        executor.execute()

        req1 = self.ctx.socket(zmq.REQ)
        req1.connect(self.ADDR_REQ)

        req2 = self.ctx.socket(zmq.REQ)
        req2.connect(self.ADDR_REQ)

        req1.send_string('Hello')
        req2.send_string('Hello')

        reply = req1.recv_string()
        self.assertEqual('Reply', reply)

        reply = req2.recv_string()
        self.assertEqual('Reply', reply)

    def test_poll_timeout(self):
        rep = self.ctx.socket(zmq.REP)
        rep.bind(self.ADDR_REQ)

        reply_poller = zmq_async.get_reply_poller()
        reply_poller.register(rep)

        incoming, socket = reply_poller.poll(1)
        self.assertIsNone(incoming)
        self.assertIsNone(socket)
