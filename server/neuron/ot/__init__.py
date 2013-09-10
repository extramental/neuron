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

    def receive_operation(self, conn_id, revision, operation):
        """Transforms an operation coming from a client against all concurrent
        operation, applies it to the current document and returns the operation
        to send to the clients.
        """

        last_by_user = self.backend.get_last_revision_from_user(conn_id)
        if last_by_user and last_by_user >= revision:
            return

        Operation = operation.__class__

        concurrent_operations = self.backend.get_operations(revision)

        for concurrent_operation in concurrent_operations:
            operation, _ = Operation.transform(operation, concurrent_operation)

        self.backend.save_operation(conn_id, operation)
        return operation


class RedisTextDocumentBackend(object):
    def __init__(self, redis, doc_id, name=None):
        self.redis = redis
        self.doc_id = doc_id
        self.name = name

        self.cursors = {}
        self.last_client_revs = {}

    def set_client(self, conn_id, last_rev):
        """
        Add or update a client in the client hash.
        """
        self.last_client_revs[conn_id] = last_rev

    def remove_client(self, conn_id):
        """
        Remove a client from the client hash.
        """
        if conn_id in self.last_client_revs:
            del self.last_client_revs[conn_id]

    def get_clients(self):
        """
        Get the list of clients.
        """
        return self.last_client_revs.keys()

    def get_client_cursor(self, conn_id):
        return self.cusrors.get(conn_id, None)

    def set_client_cursor(self, conn_id, pos, end):
        self.cursors[conn_id] = (pos, end)

    def remove_client_cursor(self, conn_id):
        if conn_id in self.cursors:
            del self.cursors[conn_id]

    def get_client_cursors(self):
        return self.cursors

    def save_operation(self, conn_id, operation):
        """Save an operation in the database."""
        _, latest = self.get_latest()

        p = self.redis.pipeline()

        p.llen(str(self.doc_id) + ":history")
        p.rpush(str(self.doc_id) + ":history",
                self._serialize_wrapped_op(self.name,
                                           int(time.time()),
                                           operation.invert(latest)))
        p.set(str(self.doc_id) + ":latest", operation(latest))
        rev, _, _ = p.execute()

        self.set_client(conn_id, rev)

    def get_operations(self, start, end=None):
        """Return operations in a given range."""
        _, w_ops, latest = self.get_history_operations_to_latest(start)

        w_ops.reverse()

        acc = []
        for _, _, op in w_ops:
            acc.append(op.invert(latest))
            latest = op(latest)
        acc.reverse()

        return acc[:end]

    def get_last_revision_from_user(self, conn_id):
        """Return the revision number of the last operation from a given user."""
        return self.last_client_revs[conn_id]

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
        p.llen(str(self.doc_id) + ":history")
        p.get(str(self.doc_id) + ":latest")
        rev_plus_one, latest = p.execute()

        return rev_plus_one - 1, (latest or "").decode("utf-8")

    def get_history_operations_to_latest(self, start):
        """
        Get the latest revision of the document, with all the operations from
        the start revision.
        """
        p = self.redis.pipeline()
        p.llen(str(self.doc_id) + ":history")
        p.lrange(str(self.doc_id) + ":history", start, -1)
        p.get(str(self.doc_id) + ":latest")
        rev_plus_one, raw_w_ops, latest = p.execute()

        return rev_plus_one - 1, \
               [self._deserialize_wrapped_op(raw_w_op)
                for raw_w_op
                in raw_w_ops], \
               (latest or b"").decode("utf-8")
