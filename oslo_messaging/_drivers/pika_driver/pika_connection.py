#    Copyright 2016 Mirantis, Inc.
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

import logging
import math
import sys
import threading

import futurist
import pika
from pika import exceptions as pika_exceptions
from pika import frame as pika_frame
from pika import spec as pika_spec
from pika.adapters import select_connection

LOG = logging.getLogger(__name__)


def _is_eventlet_monkey_patched(module):
    """Determines safely is eventlet patching for module enabled or not

    :param module: String, module name
    :return Bool, True if module is pathed, False otherwise
    """

    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched(module)


class ThreadSafeSelectConnection(pika.SelectConnection):
    def __init__(self, *args, **kwargs):
        self._channel_lock = threading.Lock()
        super(ThreadSafeSelectConnection, self).__init__(*args, **kwargs)

    def channel(self, *args, **kwargs):
        with self._channel_lock:
            return super(ThreadSafeSelectConnection, self).channel(
                *args, **kwargs
            )

    def _adapter_disconnect(self, *args, **kwargs):
        with self._write_lock:
            return super(ThreadSafeSelectConnection, self)._adapter_disconnect(
                *args, **kwargs
            )

    def _send_data(self, frames):
        with self._write_lock:
            if self.is_closed:
                LOG.critical('Attempted to send frame when closed')
                raise pika_exceptions.ConnectionClosed

            bytes_sent = 0
            for frame in frames:
                bytes_sent += len(frame)
            self.outbound_buffer += frames
            self._flush_outbound()

            self.frames_sent += len(frames)
            self.bytes_sent += bytes_sent

            if self.params.backpressure_detection:
                self._detect_backpressure()

    def _send_frame(self, frame_value):
        """This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter.

        :param frame_value: The frame to write
        :type frame_value:  pika.frame.Frame|pika.frame.ProtocolHeader
        :raises: exceptions.ConnectionClosed

        """
        self._send_data(frame_value.marshal(),)

    def _send_method(self, channel_number, method_frame, content=None):
        """Constructs a RPC method frame and then sends it to the broker.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        if not content:
            self._send_frame(pika_frame.Method(channel_number, method_frame))
            return
        self._send_message(channel_number, method_frame, content)

    def _send_message(self, channel_number, method_frame, content=None):
        """Send the message directly, bypassing the single _send_frame
        invocation by directly appending to the output buffer and flushing
        within a lock.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        length = len(content[1])
        write_buffer = [
            pika_frame.Method(channel_number, method_frame).marshal(),
            pika_frame.Header(channel_number, length, content[0]).marshal()
        ]
        if content[1]:
            chunks = int(math.ceil(float(length) / self._body_max_length))
            for chunk in range(0, chunks):
                s = chunk * self._body_max_length
                e = s + self._body_max_length
                if e > length:
                    e = length
                write_buffer.append(pika_frame.Body(channel_number,
                                                    content[1][s:e]).marshal())

        self._send_data(write_buffer)


class BlockingConnection(object):

    def __init__(self, executor, params=None):
        self.params = params
        self._closed = threading.Event()

        self._executor = executor

        future = futurist.Future()

        self._impl = pika.SelectConnection(
            parameters=params,
            on_open_callback=future.set_result,
            on_open_error_callback=lambda conn, ex: future.set_exception(ex),
            on_close_callback=self._on_close,
            stop_ioloop_on_close=False,
            custom_ioloop=(
                select_connection.SelectPoller()
                if _is_eventlet_monkey_patched("select") else
                None
            )
        )

        self._executor.submit(self._process_io)

        future.result()

    def _on_close(self, conn, reply_code, reply_text):
        self._closed.set()
        self._executor.shutdown(wait=False)

    def _process_io(self):
        while not self._closed.is_set():
            try:
                self._impl.ioloop.poll()
                self._impl.ioloop.process_timeouts()
            except BaseException:
                LOG.exception("Error during processing connection's IO")

    def close(self, *args, **kwargs):
        self._impl.close(*args, **kwargs)
        self._closed.wait()

    def channel(self, channel_number=None):
        evt = threading.Event()
        impl_channel = self._impl.channel(
            on_open_callback=lambda x: evt.set(),
            channel_number=channel_number)

        # Create our proxy channel
        channel = BlockingChannel(impl_channel, self)

        # Link implementation channel with our proxy channel
        impl_channel._set_cookie(channel)

        evt.wait()
        return channel

    @property
    def is_closed(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closing

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_open


class BlockingChannel(object):  # pylint: disable=R0904,R0902

    def __init__(self, channel_impl, connection):
        self._impl = channel_impl
        self._connection = connection

        self._delivery_confirmation = False

        self._message_returned = False
        self._current_future = None

        self._evt_closed = threading.Event()
        self._on_close_callbacks = []
        self._close_reply_code = None
        self._close_reply_text = None
        self._on_close_callback_lock = threading.Lock()
        self._impl.add_on_close_callback(self._on_close)

    def _on_close(self, channel, reply_code, reply_text):
        with self._on_close_callback_lock:
            self._close_reply_code = reply_code
            self._close_reply_text = reply_text
            self._evt_closed.set()

        for callback in self._on_close_callbacks:
            callback(self, self._close_reply_code, reply_text)

        if self._current_future:
            self._current_future.set_exception(
                pika_exceptions.ChannelClosed(reply_code, reply_text))

    def _on_message_confirmation(self, frame):
        self._current_future.set_result(frame)

    def add_on_close_callback(self, callback):
        with self._on_close_callback_lock:
            if self._evt_closed.is_set():
                callback(self, self._close_reply_code,
                         self._close_reply_text)
            else:
                self._on_close_callbacks.append(callback)

    @property
    def is_closed(self):
        return self._impl.is_closed

    @property
    def is_closing(self):
        return self._impl.is_closing

    @property
    def is_open(self):
        return self._impl.is_open

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        self._impl.close(reply_code=reply_code, reply_text=reply_text)
        self._evt_closed.wait()

    def flow(self, active):
        self._current_future = futurist.Future()
        self._impl.flow(callback=self._current_future.set_result,
                        active=active)
        return self._current_future.result()

    def basic_consume(self,  # pylint: disable=R0913
                      consumer_callback,
                      queue,
                      no_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):
        self._current_future = futurist.Future()
        self._impl.add_callback(self._current_future.set_result,
                                replies=[pika_spec.Basic.ConsumeOk],
                                one_shot=True)
        tag = self._impl.basic_consume(
            consumer_callback=consumer_callback,
            queue=queue,
            no_ack=no_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments)

        self._current_future.result()
        return tag

    def basic_cancel(self, consumer_tag):
        self._current_future = futurist.Future()
        self._impl.basic_cancel(
            callback=self._current_future.set_result,
            consumer_tag=consumer_tag,
            nowait=False)
        self._current_future.result()

    def basic_ack(self, delivery_tag=0, multiple=False):
        self._impl.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        self._impl.basic_nack(delivery_tag=delivery_tag, multiple=multiple,
                              requeue=requeue)

    def publish(self, exchange, routing_key, body,  # pylint: disable=R0913
                properties=None, mandatory=False, immediate=False):

        if self._delivery_confirmation:
            # In publisher-acknowledgments mode
            self._message_returned = False
            self._current_future = futurist.Future()

            self._impl.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=body,
                                     properties=properties,
                                     mandatory=mandatory,
                                     immediate=immediate)

            conf_method = self._current_future.result().method

            if isinstance(conf_method, pika_spec.Basic.Nack):
                raise pika_exceptions.NackError(None,)
            else:
                assert isinstance(conf_method, pika_spec.Basic.Ack), (
                    conf_method)

                if self._message_returned:
                    raise pika_exceptions.UnroutableError(None,)
        else:
            # In non-publisher-acknowledgments mode
            self._impl.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=body,
                                     properties=properties,
                                     mandatory=mandatory,
                                     immediate=immediate)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        self._current_future = futurist.Future()
        self._impl.basic_qos(callback=self._current_future.set_result,
                             prefetch_size=prefetch_size,
                             prefetch_count=prefetch_count,
                             all_channels=all_channels)
        self._current_future.result()

    def basic_recover(self, requeue=False):
        self._current_future = futurist.Future()
        self._impl.basic_recover(
            callback=lambda: self._current_future.set_result(None),
            requeue=requeue
        )
        self._current_future.result()

    def basic_reject(self, delivery_tag=None, requeue=True):
        self._impl.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    def _on_message_returned(self, *args, **kwargs):
            self._message_returned = True

    def confirm_delivery(self):
        self._current_future = futurist.Future()
        self._impl.add_callback(callback=self._current_future.set_result,
                                replies=[pika_spec.Confirm.SelectOk],
                                one_shot=True)
        self._impl.confirm_delivery(
            callback=self._on_message_confirmation,
            nowait=False)
        self._current_future.result()

        self._delivery_confirmation = True
        self._impl.add_on_return_callback(self._on_message_returned)

    def exchange_declare(self, exchange=None,  # pylint: disable=R0913
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False,
                         arguments=None, **kwargs):
        self._current_future = futurist.Future()
        self._impl.exchange_declare(
            callback=self._current_future.set_result,
            exchange=exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            nowait=False,
            arguments=arguments,
            type=kwargs["type"] if kwargs else None)

        return self._current_future.result()

    def exchange_delete(self, exchange=None, if_unused=False):
        self._current_future = futurist.Future()
        self._impl.exchange_delete(
            callback=self._current_future.set_result,
            exchange=exchange,
            if_unused=if_unused,
            nowait=False)

        return self._current_future.result()

    def exchange_bind(self, destination=None, source=None, routing_key='',
                      arguments=None):
        self._current_future = futurist.Future()
        self._impl.exchange_bind(
            callback=self._current_future.set_result,
            destination=destination,
            source=source,
            routing_key=routing_key,
            nowait=False,
            arguments=arguments)

        return self._current_future.result()

    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        arguments=None):
        self._current_future = futurist.Future()
        self._impl.exchange_unbind(
            callback=self._current_future.set_result,
            destination=destination,
            source=source,
            routing_key=routing_key,
            nowait=False,
            arguments=arguments)

        return self._current_future.result()

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False,
                      arguments=None):
        self._current_future = futurist.Future()
        self._impl.queue_declare(
            callback=self._current_future.set_result,
            queue=queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            nowait=False,
            arguments=arguments)

        return self._current_future.result()

    def queue_delete(self, queue='', if_unused=False, if_empty=False):
        self._current_future = futurist.Future()
        self._impl.queue_delete(callback=self._current_future.set_result,
                                queue=queue,
                                if_unused=if_unused,
                                if_empty=if_empty,
                                nowait=False)

        return self._current_future.result()

    def queue_purge(self, queue=''):
        self._current_future = futurist.Future()
        self._impl.queue_purge(callback=self._current_future.set_result,
                               queue=queue,
                               nowait=False)
        return self._current_future.result()

    def queue_bind(self, queue, exchange, routing_key=None,
                   arguments=None):
        self._current_future = futurist.Future()
        self._impl.queue_bind(callback=self._current_future.set_result,
                              queue=queue,
                              exchange=exchange,
                              routing_key=routing_key,
                              nowait=False,
                              arguments=arguments)
        return self._current_future.result()

    def queue_unbind(self, queue='', exchange=None, routing_key=None,
                     arguments=None):
        self._current_future = futurist.Future()
        self._impl.queue_unbind(callback=self._current_future.set_result,
                                queue=queue,
                                exchange=exchange,
                                routing_key=routing_key,
                                arguments=arguments)
        return self._current_future.result()
