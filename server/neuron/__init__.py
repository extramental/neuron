import logging
import redis
import json
import uuid

import tornado.ioloop
import tornado.web
from tornado.options import define, options

from sockjs.tornado import SockJSRouter, SockJSConnection

from .ot import Server as OTServer
from .ot import RedisTextDocumentBackend
from .ot.text_operation import TextOperation

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
    OP_CURSOR = 5
    OP_LEFT = 6
    OP_JOIN = 7

    OP_MAP = {
        OP_AUTH: "do_auth",
        OP_LOAD: "do_load",
        OP_OPERATION: "do_operation",
        OP_CURSOR: "do_cursor",
        OP_LEFT: "do_left"
    }

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.doc_ids = set([])
        self.user_id = None

    def get_document(self, doc_id):
        return OTServer(RedisTextDocumentBackend(self.application.redis, doc_id, self.name))

    @property
    def application(self):
        return self.session.server.application

    def on_message(self, msg):
        payload = json.loads(msg)
        opcode, rest = payload[0], payload[1:]
        getattr(self, self.OP_MAP[opcode])(*rest)

    def do_auth(self, name):
        self.user_id = uuid.uuid4().hex.encode("utf-8")
        self.name = name
        self.application.conns[self.user_id] = self
        self.application.redis.hset(":users", self.user_id, self.name)
        self.send(json.dumps([self.OP_AUTH]))

    def do_load(self, doc_id):
        doc = self.get_document(doc_id)

        self.doc_ids.add(doc_id)

        doc.backend.add_client(self.user_id)
        rev, content = doc.backend.get_latest()

        clients = doc.backend.get_clients()

        p = self.application.redis.pipeline()
        for c in clients:
            p.hget(":users", c)
        names = p.execute()

        self.send(json.dumps([self.OP_CONTENT, doc_id, rev,
                              {k.decode("utf-8"): {"name": names[i].decode("utf-8")}
                               for i, k
                               in enumerate(clients)
                               if k != self.user_id},
                              content]))

        self.broadcast_to_doc(doc_id,
                              [self.OP_JOIN, doc_id, self.user_id.decode("utf-8"), self.name])

    def do_operation(self, doc_id, rev, raw_op):
        doc = self.get_document(doc_id)
        op = doc.receive_operation(self.user_id, rev, TextOperation.deserialize(raw_op))

        if op is None:
            return

        self.send(json.dumps([self.OP_ACK, doc_id]))

        self.broadcast_to_doc(doc_id,
                              [self.OP_OPERATION, doc_id, op.serialize()])

    def do_cursor(self, doc_id, cursor):
        doc = self.get_document(doc_id)

        if cursor is None:
            doc.backend.remove_client_cursor(self.user_id)
        else:
            pos, end = cursor.split(",")
            doc.backend.add_client_cursor(self.user_id, int(pos), int(end))

        self.broadcast_to_doc(doc_id,
                              [self.OP_CURSOR, doc_id, self.user_id.decode("utf-8"), cursor])

    def do_left(self, doc_id):
        self.doc_ids.remove(doc_id)
        self.get_document(doc_id).backend.remove_client(self.user_id)
        self.broadcast_to_doc(doc_id,
                              [self.OP_LEFT, doc_id, self.user_id.decode("utf-8")])

    def broadcast_to_doc(self, doc_id, payload):
        doc = self.get_document(doc_id)

        for user_id in doc.backend.get_clients():
            if user_id not in self.application.conns:
                continue
            if user_id == self.user_id:
                continue
            self.application.conns[user_id].send(json.dumps(payload))

    def on_close(self):
        try:
            del self.application.conns[self.user_id]

            if self.user_id is None:
                return

            self.application.redis.hdel(":users", self.user_id)

            for doc_id in self.doc_ids:
                payload = [self.OP_LEFT, doc_id, self.user_id]
                doc = self.get_document(doc_id)

                self.get_document(doc_id).backend.remove_client(self.user_id)

                self.broadcast_to_doc(doc_id,
                                      [self.OP_LEFT, doc_id, self.user_id.decode("utf-8")])
        except Exception as e:
            logging.error("Error during on_close:", exc_info=True)


class Application(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        tornado.web.Application.__init__(self, *args, **kwargs)
        self.redis = self.settings["redis"]
        self.conns = {}


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
