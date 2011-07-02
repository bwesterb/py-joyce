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
from sarah.io import IntSocketFile, pump, CappedReadFile
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
                self.JSONP_callback = None
                self.server = server
                BaseHTTPRequestHandler.__init__(self, request, addr, server)
        def log_message(self, format, *args, **kwargs):
                self.l.info(format, *args, **kwargs)
        def log_error(self, format, *args, **kwargs):
                self.l.error(format, *args, **kwargs)
        def log_request(self, code=None, size=None):
                self.l.info("Request: %s %s" % (code, size))
        def do_OPTIONS(self):
                self.l.debug('OPTIONS')
                self.send_response(200)
                self.send_header('Access-Control-Max-Age', '1728000')
                self.send_header('Access-Control-Allow-Methods',
                                'POST, GET, OPTIONS')
                if 'Origin' in self.headers:
                        self.send_header('Access-Control-Allow-Origin',
                                self.headers.getheader('Origin'))
                if 'Access-Control-Request-Headers' in self.headers:
                        self.send_header('Access-Control-Allow-Headers',
                                self.headers.getheader(
                                        'Access-Control-Request-Headers'))
                self.end_headers()
                self.real_finish()
        def do_GET(self):
                path = urlparse.urlparse(self.path)
                qs = urlparse.parse_qs(path.query)
                if 'c' in qs and len(qs['c']) == 1:
                        self.JSONP_callback = qs['c'][0]
                if path.path == '/':
                        if not 'm' in qs or len(qs['m']) != 1:
                                return self._respond_simple(400,
                                                'Missing argument m')
                        self._dispatch_message(qs['m'][0])
                elif path.path == '/s':
                        if not 'm' in qs or len(qs['m']) != 1:
                                return self._respond_simple(400,
                                                'Missing argument m')
                        self._dispatch_stream_out(qs['m'][0])
                else:
                        self._respond_simple(400, 'Unknown path')
        def do_POST(self):
                path = urlparse.urlparse(self.path)
                qs = urlparse.parse_qs(path.query)
                if 'c' in qs and len(qs['c']) == 1:
                        self.JSONP_callback = qs['c']
                if path.path == '/':
                        if 'Content-Type' in self.headers:
                                ct = cgi.parse_header(self.headers.getheader(
                                        'Content-Type'))[0]
                                if ct in ['multipart/form-data',
                                                'application/octet-stream']:
                                        self._dispatch_stream(qs, ct)
                                        return
                                if ct != 'application/x-www-form-urlencoded':
                                        return self._respond_simple(400,
                                                'Unsupported Content-Type')
                                d = urlparse.parse_qs(self.rfile.read(
                                        int(self.headers['Content-Length'])))
                                if not 'm' in d or len(d['m']) != 1:
                                        return self._respond_simple(400,
                                                        'Missing arg m')
                                self._dispatch_message(d['m'][0])
                                return
                        if not 'Content-Length' in self.headers:
                                self._respond_simple(400, "No Content-Length")
                        self._dispatch_message(self.rfile.read(
                                int(self.headers['Content-Length'])))
                elif path.path == '/s':
                        if not 'm' in qs or len(qs['m']) != 1:
                                return self._respond_simple(400,
                                                'Missing argument m')
                        self._dispatch_stream_out(qs['m'][0])
                else:
                        self._respond_simple(400, 'Unknown path')
        def _dispatch_stream(self, qs, content_type):
                if not 'm' in qs or len(qs['m']) != 1:
                        return self._respond_simple(400,
                                        'Missing argument m')
                if not 'Content-Length' in self.headers:
                        self._respond_simple(400, 'No Content-Length')
                        return
                token = qs['m'][0]
                length = int(self.headers['Content-Length'])
                if content_type == 'multipart/form-data':
                        fs = cgi.FieldStorage(self.rfile, self.headers,
                                environ={'REQUEST_METHOD': 'POST',
                                         'CONTENT_TYPE': self.headers['Content-Type']})
                        stream = fs['stream'].file
                else:
                        stream = CappedReadFile(self.rfile, length)
                def _on_stream_closed(name, func, args, kwargs):
                        self.send_response(200)
                        self.send_header('Access-Control-Allow-Origin', '*')
                        if 'r' in qs and qs['r'][0] == 'fu':
                                self.send_header('Content-Type', 'text/html')
                                self.end_headers()
                                self.wfile.write('{"success":true}')
                        else:
                                self.end_headers()
                                self.real_finish()
                        self.real_finish()
                stream = CallCatchingWrapper(stream,
                                lambda x: x == 'close', _on_stream_closed)
                self.server.dispatch_stream(token, stream, self)
        def _dispatch_stream_out(self, v):
                try:
                        d = json.loads(v)
                except ValueError:
                        self._respond_simple(400, 'Malformed JSON')
                        return
                if len(d) != 2:
                        self._respond_simple(400, 'Malformed JSON request')
                        return
                token, streamid = d
                self.server.dispatch_stream_out(token, streamid, self)
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
                self.l.debug('_respond_simple: %s %s' % (code, repr(message)))
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
                # TODO add timeouts to streams
                self.streams = {}
                self.stream_notices = []
                self.token = token
                self.stream_counter = 0
        def send_message(self, token, data):
                if token != self.token:
                        raise ValueError, "%s != %s" % (token, self.token)
                with self.lock:
                        self.messages.append(data)
                        if not self.rh is None:
                                self.__flush()
        def send_stream(self, token, stream, blocking=True):
                if token != self.token:
                        raise ValueError, "%s != %s" % (token, self.token)
                if stream is None:
                        raise ValueError, "Stream can't be None"
                event = threading.Event() if blocking else None
                with self.lock:
                        self.stream_counter += 1
                        self.streams[self.stream_counter] = (stream, event)
                        self.stream_notices.append(self.stream_counter)
                if blocking:
                        event.wait()
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
                self.handle_stream(self.token, stream)
        def _handle_stream_out(self, rh, streamid):
                with self.lock:
                        if streamid in self.streams:
                                tmp = self.streams[streamid]
                                del self.streams[streamid]
                        else:
                                tmp = None
                if tmp is None:
                        rh._respond_simple(400, "Stream not found")
                        return
                stream, event = tmp
                rh.send_response(200)
                rh.end_headers()
                pump(stream, rh.wfile)
                rh.real_finish()
                if not event is None:
                        event.set()
        def _handle_message(self, rh, data, direct_return):
                with self.lock:
                        if not self.rh is None:
                                self.__flush()
                        else:
                                self._set_timeout(int(time.time() +
                                                self.hub.timeout))
                        self.rh = rh
                        if direct_return or self.messages:
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
                stream_notices = list(self.stream_notices)
                self.stream_notices = []
                self.messages = []
                args = (rh, messages, stream_notices)
                if async:
                        self.hub.threadPool.execute_named(self.__inner_flush,
                                '%s __inner__flush' % self.hub.l.name, *args)
                else:
                        self.__inner_flush(*args)
                self.rh = None
                self._set_timeout(int(time.time() + self.hub.timeout))
        def __inner_flush(self, rh, messages, stream_notices):
                try:
                        rh.send_response(200)
                        rh.send_header('Content-Type', 'text/javascript'
                                        if rh.JSONP_callback is not None
                                        else 'application/json')
                        rh.send_header('Access-Control-Allow-Origin','*')
                        rh.end_headers()
                        if rh.JSONP_callback is not None:
                                rh.wfile.write(rh.JSONP_callback + '(')
                        try:
                                json.dump([self.token, messages,
                                                stream_notices],
                                                rh.wfile)
                        except TypeError:
                                self.l.exception("Unserializable message?")
                        if rh.JSONP_callback is not None:
                                rh.wfile.write(');')
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
                self.cond_msg_in = threading.Condition()
                self.cond_out = threading.Condition()
                self.running = False
                self.token = token
                self.queue_msg_in = []
                self.queue_out = []
                self.nPending = 0
        def send_stream(self, token, stream, blocking=True):
                if token != self.token:
                        raise ValueError, "%s != %s" (token, self.token)
                datagen, headers = poster.encode.multipart_encode({
                                        'stream': stream})
                request = urllib2.Request("http://%s:%s%s?m=%s" % (
                                self.hub.host, self.hub.port, self.hub.path,
                                token), datagen, headers)
                resp = urllib2.urlopen(request)
                if blocking:
                        resp.read()
        def send_message(self, token, d):
                if token != self.token:
                        raise ValueError, "%s != %s" % (token, self.token)
                with self.cond_out:
                        self.queue_out.append(d)
                        self.cond_out.notify()
        def run_message_dispatcher(self):
                while self.running:
                        with self.cond_msg_in:
                                while self.queue_msg_in:
                                        self.handle_message(self.token,
                                                        self.queue_msg_in.pop())
                                self.cond_msg_in.wait()
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
                self.hub.threadPool.execute_named(self.run_message_dispatcher,
                                '%s.run_message_dispatcher' % self.l.name)
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
                if len(d) != 3:
                        raise ValueError, "Unexpected size of reponse-list"
                token, msgs, streams = d
                with self.cond_out:
                        old, self.token = self.token, token
                        if old is None:
                                self.cond_out.notify()
                if msgs:
                        with self.cond_msg_in:
                                for m in msgs:
                                        self.queue_msg_in.append(m)
                                self.cond_msg_in.notify()
                for s in streams:
                        self.hub.threadPool.execute_named(
                                self._retreive_stream,
                                '%s._retreive_stream' % self.l.name, s)
        def _retreive_stream(self, stream_id):
                conn = httplib.HTTPConnection(self.hub.host, self.hub.port)
                assert not self.token is None
                data = urllib2.quote(json.dumps([self.token, stream_id]))
                conn.request('GET', self.hub.path + 's?m=' + data)
                resp = conn.getresponse()
                self.handle_stream(self.token, resp)
        def stop(self):
                self.running = False
                with self.cond_msg_in:
                        self.cond_msg_in.notifyAll()
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
                self.threadPool.execute_named(relay.run, '%s.run' % l.name)
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
                self.sleepEvent = threading.Event()
        def _get_relay_for_token(self, token):
                with self.lock:
                        if token is None:
                                token = self._generate_token()
                        relay = self.channel_to_relay.get(token)
                if relay is None:
                        l = logging.getLogger("%s.relay.%s" % (
                                self.l.name, token))
                        relay = CometJoyceServerRelay(self, l, token)
                        self._get_channel_for_relay(token, relay)
                return relay
        def dispatch_stream(self, token, stream, rh):
                relay = self._get_relay_for_token(token)
                relay._handle_stream(rh, stream)
        def dispatch_stream_out(self, token, streamid, rh):
                relay = self._get_relay_for_token(token)
                relay._handle_stream_out(rh, streamid)
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
                        if timeout <= 0:
                                pass
                        elif timeout < 0.1:
                                time.sleep(timeout)
                        else: # 0.1 < timeout
                                self.sleepEvent.clear()
                                self.scheduler.plan(time.time() + timeout,
                                                self.sleepEvent.set)
                                self.sleepEvent.wait()
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
                self.sleepEvent.set()
                with self.lock:
                        ds = self.timeout_lut.values()
                for ss in ds:
                        for s in tuple(ss):
                                s.abort()
