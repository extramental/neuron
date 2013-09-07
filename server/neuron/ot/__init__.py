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

    def set_client(self, user_id, last_rev):
        """
        Add or update a client in the client hash.
        """
        self.last_user_revs[user_id] = last_rev

    def remove_client(self, user_id):
        """
        Remove a client from the client hash.
        """
        if user_id in self.last_user_revs:
            del self.last_user_revs[user_id]

    def get_clients(self):
        """
        Get the list of clients.
        """
        return self.last_user_revs.keys()

    def get_client_cursor(self, user_id):
        return self.cusrors.get(user_id, None)

    def set_client_cursor(self, user_id, pos, end):
        self.cursors[user_id] = (pos, end)

    def remove_client_cursor(self, user_id):
        if user_id in self.cursors:
            del self.cursors[user_id]

    def get_client_cursors(self):
        return self.cursors

    def save_operation(self, user_id, operation):
        """Save an operation in the database."""
        _, latest = self.get_latest()

        p = self.redis.pipeline()

        p.llen(self.doc_id + ":history")
        p.rpush(self.doc_id + ":history",
                self._serialize_wrapped_op(self.name, int(time.time()), operation))
        p.set(self.doc_id + ":latest", operation(latest))
        rev, _, _ = p.execute()

        self.set_client(user_id, rev)

    def get_operations(self, start, end=-1):
        """Return operations in a given range."""
        acc = []
        for raw_w_op in self.redis.lrange(self.doc_id + ":history", start, end):
            _, _, op = self._deserialize_wrapped_op(raw_w_op)
            acc.append(op)
        return acc

    def get_last_revision_from_user(self, user_id):
        """Return the revision number of the last operation from a given user."""
        return self.last_user_revs[user_id]

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
