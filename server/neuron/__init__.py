import logging
import redis
import json
import time

import tornado.ioloop
import tornado.web
from tornado.options import define, options

from sockjs.tornado import SockJSRouter, SockJSConnection

from .ot import text

define("debug", default=False, help="run in debug mode")
define("port", default=8080, help="port to run on")
define("redis_host", default="localhost", help="host redis is running on")
define("redis_port", default=6379, help="port redis is running on")
define("redis_db", default=0, help="db to use on redis")


class ConcurrentDocument(object):
    def __init__(self, redis, id):
        self.redis = redis
        self.id = id

    @staticmethod
    def deserialize_op(x):
        rev, ts, uid, op = x.split(":", 3)
        rev = int(rev)
        ts = int(ts)
        uid = int(uid)
        op = text.deserialize_op(op)

        return rev, ts, uid, op

    @staticmethod
    def make_serialized_op(rev, ts, uid, op):
        """
        Make a serialized operation for use with Redis.
        """
        return "{}:{}:{}:{}".format(rev, ts, uid, text.serialize_op(op))

    @staticmethod
    def make_minimal_rev(rev, ts, uid, content):
        """
        Make a minimal revision for use with Redis.
        """
        return "{}:{}:{}:{}".format(rev, ts, uid, content)

    NEW_DOCUMENT = make_minimal_rev.__func__(0, 0, -1, "")

    def add_client(self, uid, min_rev=0):
        """
        Add or update a client in the client hash.
        """
        self.redis.hset(self.id + ":uids", uid, min_rev)

    def remove_client(self, uid):
        """
        Remove a client from the client hash.
        """
        self.redis.hdel(self.id + ":uids", uid)

    def get_clients(self):
        """
        Get the list of clients.
        """
        return [int(x) for x in self.redis.hkeys(self.id + ":uids")]

    def get_minimal(self):
        """
        Get the minimal revision of the document. By "minimal", it means the
        revision that the slowest client is at.
        """
        minimal = self.redis.get(self.id + ":minimal")
        if minimal is None:
            minimal = self.NEW_DOCUMENT
            self.redis.set(self.id + ":minimal", minimal)
        rev, ts, uid, content = minimal.split(":", 3)
        return int(rev), int(ts), int(uid), content

    def get_last_uids(self):
        """
        Get the list of last revisions a given UID touched.
        """
        return {int(k): int(v)
                for k, v
                in self.redis.hgetall(self.id + ":uids").iteritems()}

    def get_pending(self):
        """
        Get the list of pending operations for the document.
        """
        return [self.deserialize_op(raw_op)
                for raw_op
                in self.redis.lrange(self.id + ":pending", 0, -1)]

    def get_latest(self):
        """
        Get the latest revision of the document.
        """
        rev, ts, uid, content = self.get_minimal()

        for rev, ts, uid, op in self.get_pending():
            content = op(content)

        return rev, ts, uid, content

    def get_pending_revision(self, rev):
        """
        Get a revision that can be constructed from operations in the pending
        list.
        """
        r, ts, uid, content = self.get_minimal()

        # if the minimal revision is the pending revision, then we don't have
        # to apply any pending operations
        if r == rev:
            return r, ts, uid, content

        # otherwise, we need to make it up to the pending revision
        for r, ts, uid, op in self.get_pending():
            if r > rev:
                break
            content = op(content)

        if r != rev:
            raise Exception("could not find pending revision??")

        return rev, ts, uid, content

    def reify_minimal(self):
        """
        Reify as many pending operations as possible into the minimal text.
        """
        # check if we can flush some pending operations
        min_rev, _, _, content = self.get_minimal()
        new_min_rev = min(self.get_last_uids().values())

        if new_min_rev > min_rev:
            # yes we can! we want to commit a few pending operations into
            # history now.
            n = new_min_rev - min_rev

            p = self.redis.pipeline()

            # generate historical undo operations
            for pending_rev, pending_ts, pending_uid, pending_op in self.get_pending()[:n]:
                undo_op = pending_op.invert(content)
                p.rpush(self.id + ":history",
                        self.make_serialized_op(pending_rev, pending_ts, pending_uid, undo_op))
                content = pending_op(content)

            # get rid of the pending operations we've committed into history
            for _ in range(n):
                # i would use ltrim, but all pending ops might actually be
                # removed
                p.lpop(self.id + ":pending")

            # commit a new minimal revision
            p.set(self.id + ":minimal",
                  self.make_minimal_rev(pending_rev, pending_ts, pending_uid, content))
            p.execute()

        return new_min_rev

    def run_operation(self, rev, ts, uid, op):
        """
        Run an operation on the document. Please note that rev specifies the
        revision to run the operation at, not the revision the operation
        creates.
        """
        _, _, _, content = self.get_pending_revision(rev)

        try:
            op(content)
        except text.IncompatibleOperationError as e:
            raise e

        last_by_user = self.get_last_uids()[uid]
        if last_by_user > rev:
            return

        r = rev
        for crev, _, _, cop in self.get_pending():
            if crev <= rev:
                continue
            op, _ = text.TextOperation.transform(op, cop)

            r += 1
        rev = r

        # push our operation onto the pending queue
        self.redis.rpush(self.id + ":pending",
                         self.make_serialized_op(rev + 1, ts, uid, op))
        self.add_client(uid, rev + 1)

        return op


class Connection(SockJSConnection):
    OP_AUTH = 0
    OP_LOAD = 1
    OP_CONTENT = 2
    OP_OPERATION = 3
    OP_ACK = 4

    OP_MAP = {
        OP_AUTH: "do_auth",
        OP_LOAD: "do_load",
        OP_OPERATION: "do_operation",
        OP_ACK: "do_ack"
    }

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
        doc = ConcurrentDocument(self.application.redis, doc_id)

        self.doc_ids.add(doc_id)

        doc.add_client(self.uid)
        rev, _, _, content = doc.get_latest()
        doc.add_client(self.uid, rev)

        self.send(json.dumps([self.OP_CONTENT, doc_id, rev, content]))

    def do_operation(self, doc_id, rev, raw_op):
        doc = ConcurrentDocument(self.application.redis, doc_id)
        op = doc.run_operation(rev, int(time.time()), self.uid, text.deserialize_op(raw_op))

        if op is None:
            return

        for uid in doc.get_clients():
            if uid not in self.application.uid_conns:
                doc.remove_client(uid)
                continue
            if uid == self.uid:
                continue

            payload = [self.OP_OPERATION, doc_id, rev + 1, text.serialize_op(op)]
            conn = self.application.uid_conns[uid]

            conn.send(json.dumps(payload))

        doc.reify_minimal()

        self.send(json.dumps([self.OP_ACK, doc_id, rev]))

    def do_ack(self, doc_id, rev):
        doc = ConcurrentDocument(self.application.redis, doc_id)
        doc.add_client(self.uid, rev)

    def on_close(self):
        if self.uid is None:
            return

        for doc_id in self.doc_ids:
            doc = ConcurrentDocument(self.application.redis, doc_id)
            doc.remove_client(self.uid)


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
