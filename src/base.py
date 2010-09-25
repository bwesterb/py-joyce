from mirte.core import Module

from sarah.event import Event

import os
import base64
import threading

class JoyceError(Exception):
	pass
class HijackedChannel(JoyceError):
	pass
class UnsupportedProtocol(JoyceError):
	pass

class JoyceChannel(object):
	def __init__(self, relay, token):
		self.relay = relay
		self.token = token
		self.on_message = Event()
	def send_message(self, d):
		self.relay.send_message(self.token, d)
	def handle_message(self, d):
		self.on_message(self, d)

class JoyceRelay(object):
	def __init__(self, hub):
		self.hub = hub
	def send_message(self, token, d):
		raise NotImplementedError
	def handle_message(self, token, d):
		self.hub.handle_message(token, d, self)

class JoyceHub(object):
	def __init__(self, channel_class=None):
		self.channel_class = (JoyceChannel
				if channel_class is None else channel_class)
		self.lock = threading.Lock()
		self.channel_to_relay = dict()
		self.channels = dict()
		self.on_new_channel = Event()
	def handle_message(self, token, d, relay):
		new_channel = False
		with self.lock:
			if not token in self.channels:
				c = self._create_channel(token, relay)
				new_channel = True
			else:
				if relay != self.channel_to_relay[token]:
					raise HijackedChannel
				c = self.channels[token]
		if new_channel:
			self.on_new_channel(c)
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
		channel = channel_class(relay, token)
		self.channel_to_relay[token] = relay
		self.channels[token] = channel
		return channel
	def _generate_token(self):
		while True:
			_try = base64.b64encode(os.urandom(6))
			if not _try in self.channels:
				self.channels[_try] = None
				return _try

class JoyceClient(Module, JoyceHub):
	def __init__(self, *args, **kwargs):
		Module.__init__(self, *args, **kwargs)
		JoyceHub.__init__(self)
	def create_channel(self, token=None, channel_class=None):
		raise NotImplementedError
class JoyceServer(Module, JoyceHub):
	def __init__(self, *args, **kwargs):
		Module.__init__(self, *args, **kwargs)
		JoyceHub.__init__(self)
