import cgi
import time
import json
import socket
import urllib2
import httplib
import logging
import urlparse
import threading
import poster.encode
import poster.streaminghttp

# TODO avoid this call
poster.streaminghttp.register_openers()

from BaseHTTPServer import BaseHTTPRequestHandler

from mirte.core import Module
from sarah.event import Event
from sarah.io import IntSocketFile
from sarah.runtime import CallCatchingWrapper
from sarah.socketServer import TCPSocketServer

from joyce.base import JoyceServer, JoyceClient, JoyceRelay

class CometRHWrapper(object):
	""" Exposes SocketServer handler semantics for our
	    BaseHTTPRequestHandler based CometRH """
	def __init__(self, request, addr, server, l):
		self.request = IntSocketFile(request)
		self.addr = addr
		self.server = server
		self.l = l
	def handle(self):
		self.h = CometRH(self.request, self.addr,
					self.server, self.l)
	def interrupt(self):
		self.request.interrupt()
	def cleanup(self):
		pass

class CometRH(BaseHTTPRequestHandler):
	def __init__(self, request, addr, server, l):
		self.l = l
		self.server = server
		BaseHTTPRequestHandler.__init__(self, request, addr, server)
	def log_message(self, format, *args, **kwargs):
		self.l.info(format, *args, **kwargs)
	def log_error(self, format, *args, **kwargs):
		self.l.error(format, *args, **kwargs)
	def log_request(self, code=None, size=None):
		self.l.info("Request: %s %s" % (code, size))
	def do_GET(self):
		v = urllib2.unquote(urlparse.urlparse(self.path).query)
		self._dispatch_message(v)
	def do_POST(self):
		if ('Content-Type' in self.headers and cgi.parse_header(
				self.headers.getheader('Content-Type'))[0] ==
					'multipart/form-data'):
			self._dispatch_stream()
			return
		if not 'Content-Length' in self.headers:
			self._respond_simple(400, "No Content-Length")
		self._dispatch_message(self.rfile.read(
			int(self.headers['Content-Length'])))
	def _dispatch_stream(self):
		fs = cgi.FieldStorage(self.rfile, self.headers,
			environ={'REQUEST_METHOD': 'POST',
				 'CONTENT_TYPE': self.headers['Content-Type']})
		token = urllib2.unquote(urlparse.urlparse(self.path).query)
		def _on_stream_closed(name, func, args, kwargs):
			func(*args, **kwargs)
			self._respond_simple(200,'')
		stream = CallCatchingWrapper(fs['stream'].file,
				lambda x: x == 'close', _on_stream_closed)
		self.server.dispatch_stream(token, stream, self)
	def _dispatch_message(self, v):
		if v == '':
			d = None
		else:
			try:
				d = json.loads(v)
			except ValueError:
				self._respond_simple(400, 'Malformed JSON')
				return
		self.server.dispatch_message(d, self)
	def _respond_simple(self, code, message):
		self.send_response(code)
		self.end_headers()
		self.wfile.write(message)
		self.real_finish()	
	def real_finish(self):
		BaseHTTPRequestHandler.finish(self)
	def finish(self):
		# We don't want our parent to close our wfile and rfile.
		pass

class CometJoyceServerRelay(JoyceRelay):
	def __init__(self, hub, logger, token):
		super(CometJoyceServerRelay, self).__init__(hub, logger)
		self.lock = threading.Lock()
		self.rh = None
		self.timeout = None
		self.messages = []
		self.token = token
	def send_message(self, token, data):
		if token != self.token:
			raise ValueError, "%s != %s" % (token, self.token)
		with self.lock:
			self.messages.append(data)
			if not self.rh is None:
				self.__flush()
	def _set_timeout(self, timeout):
		if not timeout == self.timeout:
			if not self.timeout is None:
				self.hub.remove_timeout(self.timeout, self)
			self.hub.add_timeout(timeout, self)
			self.timeout = timeout
	def _handle_stream(self, rh, stream):
		with self.lock:
			self._set_timeout(int(time.time() +
						self.hub.timeout))
		self.handle_stream(stream)
	def _handle_message(self, rh, data, direct_return):
		with self.lock:
			if not self.rh is None:
				self.__flush()
			else:
				self._set_timeout(int(time.time() +
						self.hub.timeout))
			self.rh = rh
			if direct_return:
				self.__flush()
		if len(data) > 1:
			for d in data[1:]:
				self.handle_message(self.token, d)
	def on_timeout(self, timeout):
		with self.lock:
			if timeout != self.timeout:
				return
			if self.rh is None:
				self.hub.remove_channel(self.token)
			else:
				self.__flush()
	def flush(self):
		with self.lock:
			if not self.rh is None:
				self.__flush()
	def __flush(self, async=True):
		""" Flushes messages through current HttpRequest and closes it.
		    It assumes a current requesthandler and requires a lock
		    on self.lock """
		rh = self.rh
		messages = list(self.messages)
		self.messages = []
		args = (rh, messages)
		if async:
			self.hub.threadPool.execute_named(self.__inner_flush,
				'%s __iner__flush' % self.hub.l.name, *args)
		else:
			self.__inner_flush(*args)
		self.rh = None
		self._set_timeout(int(time.time() + self.hub.timeout))
	def __inner_flush(self, rh, messages):
		try:
			rh.send_response(200)
			rh.end_headers()
			json.dump([self.token] + messages, rh.wfile)
			rh.real_finish()
		except socket.error:
			self.l.exception("Exception while flushing")
	def abort(self):
		with self.lock:
			if not self.rh is None:
				self.__flush(async=False)

class CometJoyceClientRelay(JoyceRelay):
	def __init__(self, hub, logger, token=None):
		super(CometJoyceClientRelay, self).__init__(hub, logger)
		self.cond_in = threading.Condition()
		self.cond_out = threading.Condition()
		self.running = False
		self.token = token
		self.queue_in = []
		self.queue_out = []
		self.nPending = 0
	def send_stream(self, token, stream, blocking=True):
		if token != self.token:
			raise ValueError, "%s != %s" (token, self.token)
		datagen, headers = poster.encode.multipart_encode({
					'stream': stream})
		request = urllib2.Request("http://%s:%s%s?%s" % (
				self.host, self.port, self.path, token),
				datagen, headers)
		resp = urllib2.urlopen(request)
		if blocking:
			json.loads(resp.read())

	def send_message(self, token, d):
		if token != self.token:
			raise ValueError, "%s != %s" % (token, self.token)
		with self.cond_out:
			self.queue_out.append(d)
			self.cond_out.notify()
	def run_dispatcher(self):
		while self.running:
			with self.cond_in:
				while self.queue_in:
					self.handle_message(self.token,
							self.queue_in.pop())
				self.cond_in.wait()
	def run_requester(self):
		while self.running:
			data = None
			with self.cond_out:
				if ((not self.queue_out or self.token is None)
						and self.nPending > 0):
					self.cond_out.wait()
					if not self.running:
						return
					continue
				self.nPending += 1
				if self.queue_out:
					data = list(self.queue_out)
					self.queue_out = []
			self._do_request(data)
			with self.cond_out:
				self.nPending -= 1
	def run(self):
		assert not self.running
		self.running = True
		if self.token is None:
			self._do_request()
		self.hub.threadPool.execute_named(self.run_dispatcher,
				'%s.run_dispatcher' % self.l.name)
		self.hub.threadPool.execute_named(self.run_requester,
				'%s.run_requester' % self.l.name)
		self.run_requester()
	def _do_request(self, data=None):
		conn = httplib.HTTPConnection(self.hub.host, self.hub.port)
		if data is None and not self.token is None:
			data = [self.token] 
		else:
			data.append(self.token)
		data = json.dumps(list(reversed(data)))
		conn.request('POST', self.hub.path, data)
		resp = conn.getresponse()
		d = json.load(resp)
		with self.cond_out:
			old, self.token = self.token, d[0]
			if old is None:
				self.cond_out.notify()
		if len(d) == 1:
			return
		with self.cond_in:
			for m in d[1:]:
				self.queue_in.append(m)
			self.cond_in.notify()
	def stop(self):
		self.running = False
		with self.cond_in:
			self.cond_in.notifyAll()
		with self.cond_out:
			self.cond_out.notifyAll()


class CometJoyceClient(JoyceClient):
	def __init__(self, *args, **kwargs):
		super(CometJoyceClient, self).__init__(*args, **kwargs)
		self.running = True
	def create_channel(self, token=None, channel_class=None):
		with self.lock:
			if not self.running:
				return
			if token is None:
				token = self._generate_token()
			l = logging.getLogger("%s.relay-%s" % (
				self.l.name, token))
			relay = CometJoyceClientRelay(self, l, token)
			c = self._create_channel(token, relay, channel_class)
		self.threadPool.execute_named(relay.run,
				'%s.run' % l.name)
		return c
	def run(self):
		pass
	def stop(self):
		with self.lock:
			self.running = False
			rs = self.channel_to_relay.values()
		for r in rs:
			r.stop()


class CometJoyceServer(TCPSocketServer, JoyceServer):
	def __init__(self, *args, **kwargs):
		super(CometJoyceServer, self).__init__(*args, **kwargs)
		self.timeout_lut = dict()
	def _get_relay_for_token(self, token):
		with self.lock:
			if token is None:
				token = self._generate_token()
			relay = self.channel_to_relay.get(token)
		if relay is None:
			l = logging.getLogger("%s.relay.%s" % (
				self.l.name, token))
			relay = CometJoyceServerRelay(self, l, token)
		return relay
	def dispatch_stream(self, token, stream, rh):
		relay = self._get_relay_for_token(token)
		relay._handle_stream(rh, stream)
	def dispatch_message(self, d, rh):
		direct_return = False
		if d is None:
			d = [] 
		if not isinstance(d, list):
			rh._respond_simple(400, 'Message isn\'t list')
			return
		if len(d) == 0:
			d = [None]
			direct_return = True
		relay = self._get_relay_for_token(d[0])
		relay._handle_message(rh, d, direct_return)
	def create_handler(self, con, addr, logger):
		return CometRHWrapper(con, addr, self, logger)
	def remove_timeout(self, timeout, channel):
		with self.lock:
			if timeout in self.timeout_lut:
				self.timeout_lut[timeout].remove(channel)
	def add_timeout(self, timeout, channel):
		with self.lock:
			if not timeout in self.timeout_lut:
				self.timeout_lut[timeout] = set()
			self.timeout_lut[timeout].add(channel)
	def run(self):
		# We assume SocketServer.run returns directly
		super(CometJoyceServer, self).run()
		while True:
			with self.lock:
				if not self.running:
					break
				ds = self.timeout_lut.items()
			tmp = sorted([x[0] for x in ds if x[1]])
			timeout = tmp[0] - time.time() if tmp else self.timeout
			if timeout > 0:
				time.sleep(timeout)
			with self.lock:
				if not self.running:
					break
				ds = self.timeout_lut.items()
			ds.sort(cmp=lambda x,y: cmp(x[0], y[0]))
			now = time.time()
			for t, ss in ds:
				if t >= now:
					break
				del self.timeout_lut[t]
				for s in ss:
					s.on_timeout(t)
	def stop(self):
		super(CometJoyceServer, self).stop()
		with self.lock:
			ds = self.timeout_lut.values()
		for ss in ds:
			for s in tuple(ss):
				s.abort()
