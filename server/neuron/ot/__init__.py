# Code heavily based on ot.py
# https://github.com/Operational-Transformation/ot.py

import time
import logging

from . import text_operation

class Server(object):
    """Receives operations from clients, transforms them against all
    concurrent operations and sends them back to all clients.
    """

    def __init__(self, backend):
        self.backend = backend

    def receive_operation(self, user_id, revision, operation):
        """Transforms an operation coming from a client against all concurrent
        operation, applies it to the current document and returns the operation
        to send to the clients.
        """

        last_by_user = self.backend.get_last_revision_from_user(user_id)
        if last_by_user and last_by_user >= revision:
            return

        Operation = operation.__class__

        concurrent_operations = self.backend.get_operations(revision)

        for concurrent_operation in concurrent_operations:
            operation, _ = Operation.transform(operation, concurrent_operation)

        self.backend.save_operation(user_id, operation)
        return operation


class RedisTextDocumentBackend(object):
    def __init__(self, redis, doc_id, name):
        self.redis = redis
        self.doc_id = doc_id
        self.name = name

        # TODO: the rest of this
        self.cursors = {}
        self.last_user_revs = {}

    def add_client(self, user_id, min_rev=-1):
        """
        Add or update a client in the client hash.
        """
        self.redis.hset(self.doc_id + ":user_ids", user_id, min_rev)

    def remove_client(self, user_id):
        """
        Remove a client from the client hash.
        """
        self.redis.hdel(self.doc_id + ":user_ids", user_id)

    def get_clients(self):
        """
        Get the list of clients.
        """
        return self._get_last_user_ids().keys()

    def get_client_cursor(self, user_id):
        c = self.redis.hget(self.doc_id + ":cursors", user_id)
        if not c:
            return None
        pos, end = c.split(",")
        return int(pos), int(end)

    def add_client_cursor(self, user_id, pos, end):
        self.redis.hset(self.doc_id + ":cursors", user_id, "{},{}".format(pos, end))

    def remove_client_cursor(self, user_id):
        self.redis.hdel(self.doc_id + ":cursors", user_id)

    def get_client_cursors(self):
        acc = {}
        for k, c in self.redis.hgetall(self.doc_id + ":cursors").items():
            pos, end = c.split(",")
            acc[int(k)] = (int(pos), int(end))
        return acc

    def save_operation(self, user_id, operation):
        """Save an operation in the database."""
        _, latest = self.get_latest()

        p = self.redis.pipeline()

        p.llen(self.doc_id + ":history")
        p.rpush(self.doc_id + ":history",
                self._serialize_wrapped_op(self.name, int(time.time()), operation))
        p.set(self.doc_id + ":latest", operation(latest))
        rev, _, _ = p.execute()

        self.add_client(user_id, rev)

    def get_operations(self, start, end=None):
        """Return operations in a given range."""
        acc = []
        for raw_w_op in self.redis.lrange(self.doc_id + ":history", start, end or -1):
            _, _, op = self._deserialize_wrapped_op(raw_w_op)
            acc.append(op)
        return acc

    def get_last_revision_from_user(self, user_id):
        """Return the revision number of the last operation from a given user."""
        return int(self.redis.hget(self.doc_id + ":user_ids", user_id))

    @staticmethod
    def _deserialize_wrapped_op(x):
        name, ts, op = x.decode("utf-8").split(":", 2)
        return name, int(ts), text_operation.TextOperation.deserialize(op)

    @staticmethod
    def _serialize_wrapped_op(name, ts, op):
        return "{}:{}:{}".format(name, ts, op.serialize())

    def get_latest(self):
        """
        Get the latest revision of the document.
        """
        p = self.redis.pipeline()
        p.llen(self.doc_id + ":history")
        p.get(self.doc_id + ":latest")
        rev_plus_one, latest = p.execute()

        return rev_plus_one - 1, (latest or b"").decode("utf-8")

    def _get_last_user_ids(self):
        """
        Get the list of last revisions a given UID touched.
        """
        return {k: int(v)
                for k, v
                in self.redis.hgetall(self.doc_id + ":user_ids").items()}
