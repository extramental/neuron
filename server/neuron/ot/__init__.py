# Code heavily based on ot.py
# https://github.com/Operational-Transformation/ot.py

import time

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
            (operation, _) = Operation.transform(operation, concurrent_operation)

        self.backend.save_operation(user_id, operation)
        return operation


class RedisTextDocumentBackend(object):
    def __init__(self, redis, doc_id, editor_id):
        self.redis = redis
        self.doc_id = doc_id
        self.editor_id = editor_id

    def add_client(self, user_id, min_rev=-1):
        """
        Add or update a client in the client hash.
        """
        self.redis.hset(self.doc_id + ":user_ids", user_id, str(min_rev))

    def remove_client(self, user_id):
        """
        Remove a client from the client hash.
        """
        self.redis.hdel(self.doc_id + ":user_ids", user_id)

    def get_clients(self):
        """
        Get the list of clients.
        """
        return [int(x) for x in self.redis.hkeys(self.doc_id + ":user_ids")]

    def get_client_cursor(self, user_id):
        c = self.redis.hget(self.doc_id + ":cursors", user_id).split(":")
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
        for k, c in self.redis.hgetall(self.doc_id + ":cursors"):
            pos, end = c.split(",")
            acc[int(k)] = (int(pos), int(end))
        return acc

    def save_operation(self, user_id, operation):
        """Save an operation in the database."""
        minimal, pending = self._get_minimal_and_pending()

        _, rev, _, _ = pending[-1] if pending else minimal
        rev += 1

        self.redis.rpush(self.doc_id + ":pending",
                         self._serialize_wrapped_op(self.editor_id, rev, int(time.time()), operation))

        self.add_client(user_id, rev)
        self._reify_minimal()

    def get_operations(self, start, end=None):
        """Return operations in a given range."""
        if end is None:
            end = float("inf") # XXX: lol
        return [op for _, rev, _, op in self._get_pending() if start <= rev < end]

    def get_last_revision_from_user(self, user_id):
        """Return the revision number of the last operation from a given user."""
        return int(self.redis.hget(self.doc_id + ":user_ids", user_id))

    @staticmethod
    def _deserialize_wrapped_op(x):
        editor_id, rev, ts, op = x.decode("utf-8").split(":", 3)
        return editor_id, int(rev), int(ts), text_operation.TextOperation.deserialize(op)

    @staticmethod
    def _serialize_wrapped_op(editor_id, rev, ts, op):
        return "{}:{}:{}:{}".format(editor_id, rev, ts, op.serialize())

    @staticmethod
    def _parse_minimal(raw_minimal):
        editor_id, rev, ts, content = raw_minimal.split(":", 3)
        return editor_id, int(rev), int(ts), content

    @staticmethod
    def _format_minimal(editor_id, rev, ts, content):
        """
        Make a minimal revision for use with Redis.
        """
        return "{}:{}:{}:{}".format(editor_id, rev, ts, content.encode("utf-8"))

    NEW_DOCUMENT = _format_minimal.__func__(0, 0, 0, "")

    def _get_minimal_and_pending(self):
        """
        Get both the minimal text and pending operations, atomically.
        """
        p = self.redis.pipeline()
        p.get(self.doc_id + ":minimal")
        p.lrange(self.doc_id + ":pending", 0, -1)
        raw_minimal, raw_pending = p.execute()
        if raw_minimal is None:
            raw_minimal = self.NEW_DOCUMENT
            self.redis.set(self.doc_id + ":minimal", raw_minimal)

        return self._parse_minimal(raw_minimal.decode("utf-8")), [self._deserialize_wrapped_op(raw_op)
                                                                  for raw_op in raw_pending]

    def _get_minimal(self):
        """
        Get the minimal revision of the document. By "minimal", it means the
        revision that the slowest client is at.
        """
        raw_minimal = self.redis.get(self.doc_id + ":minimal")
        if raw_minimal is None:
            raw_minimal = self.NEW_DOCUMENT
            self.redis.set(self.doc_id + ":minimal", raw_minimal)
        return self._parse_minimal(raw_minimal.decode("utf-8"))

    def _get_pending(self):
        """
        Get the list of pending operations for the document.
        """
        return [self._deserialize_wrapped_op(raw_op)
                for raw_op
                in self.redis.lrange(self.doc_id + ":pending", 0, -1)]

    def get_latest(self):
        """
        Get the latest revision of the document.
        """
        minimal, pending = self._get_minimal_and_pending()

        _, rev, _, content = minimal
        for _, rev, _, op in pending:
            content = op(content)

        return rev, content

    def _get_last_user_ids(self):
        """
        Get the list of last revisions a given UID touched.
        """
        return {int(k): int(v)
                for k, v
                in self.redis.hgetall(self.doc_id + ":user_ids").iteritems()}

    def _reify_minimal(self):
        """
        Reify as many pending operations as possible into the minimal text.
        """
        # check if we can flush some pending operations
        _, min_rev, _, content = self._get_minimal()
        new_min_rev = min(self._get_last_user_ids().values())

        if new_min_rev > min_rev:
            # yes we can! we want to commit a few pending operations into
            # history now.
            n = new_min_rev - min_rev

            pending = self._get_pending()

            p = self.redis.pipeline()

            # generate historical undo operations
            for pending_editor_id, pending_rev, pending_ts, pending_op in pending[:n]:
                undo_op = pending_op.invert(content)
                p.rpush(self.doc_id + ":history",
                        self._serialize_wrapped_op(pending_editor_id, pending_rev, pending_ts, undo_op))
                content = pending_op(content)

            # get rid of the pending operations and commit them into history
            for _ in range(n):
                # i would use ltrim, but all pending ops might actually need to
                # be removed -- ltrim retains at least one operation
                p.lpop(self.doc_id + ":pending")

            # commit a new minimal revision
            p.set(self.doc_id + ":minimal",
                  self._format_minimal(pending_editor_id, pending_rev, pending_ts, content))
            p.execute()

        return new_min_rev
