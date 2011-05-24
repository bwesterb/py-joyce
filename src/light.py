from joyce.base import JoyceServer, JoyceClient, JoyceRelay, JoyceHub, \
                        UnsupportedProtocol

from sarah.socketServer import TCPSocketServer
from sarah.io import IntSocketFile
from sarah.pack import read_packed_int, write_packed_int
from mirte.core import Module

import pymongo.bson

import json
import zlib
import struct
import socket
import logging
import threading
import cStringIO as StringIO

class LightJoyceRelay(JoyceRelay):
        def __init__(self, hub, logger, f, protocol):
                super(LightJoyceRelay, self).__init__(hub, logger)
                self.running = True
                self.queue = []
                self.cond = threading.Condition()
                self.f = f
                self.protocol = protocol
                self.cleaned = False
                self.prot_map = {
                        'json1': (self.read_json1, self.write_json1),
                        'json1gz': (self.read_json1gz, self.write_json1gz),
                        'bson1': (self.read_bson1, self.write_bson1),
                        'bson1gz': (self.read_bson1gz, self.write_bson1gz)}
        def send_message(self, token, d):
                with self.cond:
                        self.queue.append((token, d))
                        self.cond.notify()
        
        def cleanup(self):
                with self.cond:
                        if self.cleaned:
                                return
                        self.cleaned = True
                self.f.close()
                self.hub.remove_relay(self)
        def interrupt(self):
                with self.cond:
                        self.running = False
                        self.cond.notifyAll()
                self.f.interrupt()
                self.cleanup()
        def _run_sender(self):
                self.cond.acquire()
                while self.running:
                        while not self.queue and self.running:
                                self.cond.wait()
                        if not self.running:
                                break
                        queue = list(self.queue)
                        self.queue = []
                        self.cond.release()
                        try:
                                self.prot_map[self.protocol][1](queue)
                        except Exception as e:
                                self.l.exception("Uncaught exception")
                                break
                        finally:
                                self.cond.acquire()
                running = self.running
                self.cond.release()
                if running:
                        self.interrupt()
        def _run_receiver(self):
                try:
                        self.prot_map[self.protocol][0]()
                except Exception as e:
                        self.l.exception("Uncaught exception")
                if self.running:
                        self.interrupt()
        def _read_json1(self, raw_d):
                self._read_son1(json.loads(raw_d))
        def _read_son1(self, d):
                if not isinstance(d, list) or len(d) != 2:
                        self.f.write("Expected 2el list\n")
                        return
                if not isinstance(d[0], str) or d[0] is None:
                        self.f.write("Expected token\n")
                        return
                self.handle_message(*d)
        def write_json1(self, ds):
                for token, d in ds:
                        self.f.write(json.dumps([token, d]))
                        self.f.write("\n")
        def read_json1(self):
                while self.running:
                        self._read_json1(self.f.readline()[:-1])
        def write_json1gz(self, ds):
                io = StringIO.StringIO()
                first = True
                for token, d in ds:
                        if first: first = False
                        else: io.write("\n")
                        io.write(json.dumps([token, d]))
                ret = zlib.compress(io.getvalue(), 9)
                write_packed_int(self.f, len(ret))
                self.f.write(ret)
        def read_json1gz(self):
                while self.running:
                        c = read_packed_int(self.f)
                        for m in zlib.decompress(
                                        self.f.read(c)).split("\n"):
                                self._read_json1(m)
        def _read_bson1(self, raw_d):
                v = pymongo.bson.BSON(raw_d).to_dict()
                self.handle_message(v.get('t'), v.get('d'))
        def write_bson1(self, ds):
                for token, d in ds:
                        _d = {'t': token, 'd': d} 
                        tmp = str(pymongo.bson.BSON.from_dict(_d))
                        write_packed_int(self.f, len(tmp))
                        self.f.write(tmp)
        def read_bson1(self):
                while self.running:
                        c = read_packed_int(self.f)
                        self._read_bson1(self.f.read(c))
        def write_bson1gz(self, ds):
                io = StringIO.StringIO()
                for token, d in ds:
                        _d = {'t': token, 'd':d}
                        tmp = str(pymongo.bson.BSON.from_dict(_d))
                        write_packed_int(io, len(tmp))
                        io.write(tmp)
                tmp = zlib.compress(io.getvalue(), 9)
                write_packed_int(self.f, len(tmp))
                self.f.write(tmp)
        def read_bson1gz(self):
                while self.running:
                        c = read_packed_int(self.f)
                        s = zlib.decompress(self.f.read(c))
                        io = StringIO.StringIO(s)
                        while io.tell() != len(s):
                                c = read_packed_int(io)
                                self._read_bson1(io.read(c))

class LightJoyceClient(JoyceClient):
        def __init__(self, *args, **kwargs):
                super(LightJoyceClient, self).__init__(*args, **kwargs)
                self.relay = None
                self.connected = threading.Event()
                self.running = True
        def create_channel(self, token=None, channel_class=None):
                while self.relay is None:
                        self.l.warn("create_channel: not connected yet")
                        self.connected.wait()
                        if not self.running:
                                return
                with self.lock:
                        return self._create_channel(token, self.relay,
                                        channel_class)
        def run(self):
                self.do_handshake()
                self.threadPool.execute_named(self.relay._run_sender,
                                "%s _run_server" % self.l.name)
                self.relay._run_receiver()
        def do_handshake(self):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.host, self.port))
                f = IntSocketFile(s)
                prots = json.loads(f.readline()[:-1])
                if not self.protocol in prots:
                        raise UnsupportedProtocol
                f.write(json.dumps(self.protocol))
                f.write("\n")
                l = logging.getLogger("%s.relay" % self.l.name)
                self.relay = LightJoyceRelay(self, l, f, self.protocol)
                self.connected.set()
        def stop(self):
                self.running = False
                self.connected.set()
                self.relay.interrupt()
                self.relay.cleanup()

class LightJoyceServer(TCPSocketServer, JoyceServer):
        class Handler(object):
                def __init__(self, hub, conn, addr, logger):
                        self.hub = hub
                        self.conn = conn
                        self.l = logger
                def handle(self):
                        f = IntSocketFile(self.conn)
                        self.relay = LightJoyceRelay(self.hub, self.l, f, None)
                        f.write(json.dumps(self.relay.prot_map.keys()))
                        f.write("\n")
                        protocol = self.protocol = json.loads(
                                        f.readline()[:-1])
                        if not isinstance(protocol, basestring) or \
                                        not protocol in self.relay.prot_map:
                                self.f.write("Invalid response\n")
                                return
                        self.relay.protocol = protocol
                        self.hub.threadPool.execute(self.relay._run_sender)
                        self.relay._run_receiver()
                def cleanup(self):
                        self.relay.cleanup()
                def interrupt(self):
                        self.relay.interrupt()
        def __init__(self, *args, **kwargs):
                super(LightJoyceServer, self).__init__(*args, **kwargs)
        def create_handler(self, con, addr, logger):
                return LightJoyceServer.Handler(self, con, addr, logger)
