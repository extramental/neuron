import logging
import redis
import json

import tornado.ioloop
import tornado.web
from tornado.options import define, options

from sockjs.tornado import SockJSRouter, SockJSConnection

from .optrans import Server as OTServer
from .optrans import RedisTextDocumentBackend, MemoryBackend, serialize_op, deserialize_op

define("debug", default=False, help="run in debug mode")
define("port", default=8080, help="port to run on")
define("redis_host", default="localhost", help="host redis is running on")
define("redis_port", default=6379, help="port redis is running on")
define("redis_db", default=0, help="db to use on redis")


class Connection(SockJSConnection):
    OP_AUTH = 0
    OP_LOAD = 1
    OP_CONTENT = 2
    OP_OPERATION = 3
    OP_ACK = 4

    OP_MAP = {
        OP_AUTH: "do_auth",
        OP_LOAD: "do_load",
        OP_OPERATION: "do_operation"
    }

    docs = {}

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.doc_ids = set([])
        self.uid = None

    @property
    def application(self):
        return self.session.server.application

    def on_message(self, msg):
        payload = json.loads(msg)
        opcode, rest = payload[0], payload[1:]
        getattr(self, self.OP_MAP[opcode])(*rest)

    def do_auth(self, uid):
        self.uid = int(uid)
        self.application.uid_conns[uid] = self
        self.send(json.dumps([self.OP_AUTH]))

    def do_load(self, doc_id):
        if doc_id not in self.docs:
            self.docs[doc_id] = OTServer(RedisTextDocumentBackend(self.application.redis, doc_id))
        doc = self.docs[doc_id]

        self.doc_ids.add(doc_id)

        doc.backend.add_client(self.uid)
        rev, content = doc.backend.get_latest()
        doc.backend.add_client(self.uid, rev)

        self.send(json.dumps([self.OP_CONTENT, doc_id, rev, content]))

    def do_operation(self, doc_id, rev, raw_op):
        doc = self.docs[doc_id]
        op = doc.receive_operation(self.uid, rev, deserialize_op(raw_op))

        if op is None:
            return

        self.send(json.dumps([self.OP_ACK, doc_id]))

        for uid in doc.backend.get_clients():
            if uid not in self.application.uid_conns:
                doc.backend.remove_client(uid)
                continue
            if uid == self.uid:
                continue

            payload = [self.OP_OPERATION, doc_id, serialize_op(op)]
            conn = self.application.uid_conns[uid]

            conn.send(json.dumps(payload))

    def on_close(self):
        if self.uid is None:
            return

        for doc_id in self.doc_ids:
            self.docs[doc_id].remove_client(self.uid)


class Application(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        tornado.web.Application.__init__(self, *args, **kwargs)
        self.redis = self.settings["redis"]
        self.uid_conns = {}


def make_application():
    router = SockJSRouter(Connection, "")
    app = Application(router.urls, redis=redis.StrictRedis(host=options.redis_host,
                                                           port=options.redis_port,
                                                           db=options.redis_db),
                      debug=options.debug)
    # urk, yuck!
    router.application = app
    return app


def main():
    tornado.options.parse_command_line()
    application = make_application()

    application.listen(options.port)
    logging.info("Starting Neuron server on port {}...".format(options.port))
    tornado.ioloop.IOLoop.instance().start()
