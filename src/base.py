from mirte.core import Module

from sarah.event import Event

import os
import base64
import logging
import threading

class JoyceError(Exception):
        pass
class HijackedChannel(JoyceError):
        pass
class UnsupportedProtocol(JoyceError):
        pass

class JoyceChannel(object):
        def __init__(self, relay, token, logger):
                self.relay = relay
                self.l = logger
                self.token = token
                self.on_message = Event()
                self.on_stream = Event()
        def send_stream(self, stream, blocking=True):
                self.relay.send_stream(self.token, stream, blocking)
        def send_message(self, d):
                self.relay.send_message(self.token, d)
        def handle_message(self, d):
                self.on_message(self, d)
        def handle_stream(self, stream):
                self.on_stream(self, stream)
        def close(self):
                self.relay.close_channel(self.token)
        def after_close(self):
                pass

class JoyceRelay(object):
        def __init__(self, hub, logger):
                self.l = logger
                self.hub = hub
        def send_message(self, token, d):
                raise NotImplementedError
        def send_stream(self, token, stream, blocking=True):
                raise NotImplementedError
        def handle_message(self, token, d):
                self.hub.handle_message(token, d, self)
        def handle_stream(self, token, stream):
                self.hub.handle_stream(token, stream, self)
        def close_channel(self, token):
                self.hub.remove_channel(token)

class JoyceHub(Module):
        def __init__(self, channel_class=None, *args, **kwargs):
                super(JoyceHub, self).__init__(*args, **kwargs)
                self.channel_class = (JoyceChannel
                                if channel_class is None else channel_class)
                self.lock = threading.Lock()
                self.channel_to_relay = dict()
                self.relay_to_channels = dict()
                self.channels = dict()
                self.on_new_channel = Event()
        def _get_channel_for_relay(self, token, relay):
                new_channel = False
                with self.lock:
                        if self.channels.get(token) is None:
                                c = self._create_channel(token, relay)
                                new_channel = True
                        else:
                                if relay != self.channel_to_relay[token]:
                                        raise HijackedChannel
                                c = self.channels[token]
                if new_channel:
                        self.on_new_channel(c)
                return c
        def handle_stream(self, token, stream, relay):
                c = self._get_channel_for_relay(token, relay)
                c.handle_stream(stream)
        def handle_message(self, token, d, relay):
                c = self._get_channel_for_relay(token, relay)
                c.handle_message(d)
        def broadcast_message(self, d):
                with self.lock:
                        channels = self.channels.values()
                for channel in channels:
                        channel.send_message(d)
        
        def _create_channel(self, token, relay, channel_class=None):
                if token is None:
                        token = self._generate_token()
                if channel_class is None:
                        channel_class = self.channel_class
                channel = channel_class(relay, token,
                        logging.getLogger("%s.%s" % (self.l.name, token)))
                self.channel_to_relay[token] = relay
                if not relay in self.relay_to_channels:
                        self.relay_to_channels[relay] = set()
                self.relay_to_channels[relay].add(token)
                self.channels[token] = channel
                return channel
        def _generate_token(self):
                while True:
                        _try = base64.b64encode(os.urandom(6))
                        if not _try in self.channels:
                                self.channels[_try] = None
                                return _try
        def remove_channel(self, token):
                with self.lock:
                        c = self.channels[token]
                        del self.channels[token]
                        relay = self.channel_to_relay[token]
                        self.relay_to_channels[relay].remove(token)
                        del self.channel_to_relay[token]
                c.after_close()
        def remove_relay(self, relay):
                with self.lock:
                        cs = self.relay_to_channels[relay]
                        for t in cs:
                                del self.channels[t]
                                assert self.channel_to_relay[t] is relay
                                del self.channel_to_relay[t]
                        del self.relay_to_channels[relay]

class JoyceClient(JoyceHub):
        def __init__(self, *args, **kwargs):
                super(JoyceClient, self).__init__(None, *args, **kwargs)
        def create_channel(self, token=None, channel_class=None):
                raise NotImplementedError
class JoyceServer(JoyceHub):
        def __init__(self, *args, **kwargs):
                super(JoyceServer, self).__init__(None, *args, **kwargs)
