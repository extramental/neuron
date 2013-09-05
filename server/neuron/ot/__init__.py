import time
from . import text

class RedisTextDocumentBackend(object):
    def __init__(self, redis, doc_id):
        self.redis = redis
        self.doc_id = doc_id

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
        return [int(x) for x in self.redis.hkeys(self.doc_id + ":user_ids")]

    def save_operation(self, user_id, operation):
        """Save an operation in the database."""
        minimal, pending = self._get_minimal_and_pending()

        _, rev, _, _ = pending[-1] if pending else minimal
        rev += 1

        self.redis.rpush(self.doc_id + ":pending",
                         self._serialize_op(user_id, rev, int(time.time()), operation))

        self.add_client(user_id, rev)
        #self._reify_minimal()

    def get_operations(self, start, end=None):
        """Return operations in a given range."""
        if end is None:
            end = float("inf") # XXX: lol
        return [op for _, rev, _, op in self._get_pending() if start <= rev < end]

    def get_last_revision_from_user(self, user_id):
        """Return the revision number of the last operation from a given user."""
        return int(self.redis.hget(self.doc_id + ":user_ids", user_id))

    @staticmethod
    def _deserialize_op(x):
        user_id, rev, ts, op = x.split(":", 3)
        return int(user_id), int(rev), int(ts), text.deserialize_op(op)

    @staticmethod
    def _serialize_op(user_id, rev, ts, op):
        return "{}:{}:{}:{}".format(user_id, rev, ts, text.serialize_op(op))

    @staticmethod
    def _parse_minimal(raw_minimal):
        user_id, rev, ts, content = raw_minimal.split(":", 3)
        return int(user_id), int(rev), int(ts), content

    @staticmethod
    def _format_minimal(user_id, rev, ts, content):
        """
        Make a minimal revision for use with Redis.
        """
        return "{}:{}:{}:{}".format(user_id, rev, ts, content)

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

        return self._parse_minimal(raw_minimal), [self._deserialize_op(raw_op)
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
        return self._parse_minimal(raw_minimal)

    def _get_pending(self):
        """
        Get the list of pending operations for the document.
        """
        return [self._deserialize_op(raw_op)
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
            for pending_rev, pending_ts, pending_user_id, pending_op in pending[:n]:
                undo_op = pending_op.invert(content)
                p.rpush(self.doc_id + ":history",
                        self._serialize_op(pending_rev, pending_ts, pending_user_id, undo_op))
                content = pending_op(content)

            # get rid of the pending operations we've committed into history
            for _ in range(n + 1):
                # i would use ltrim, but all pending ops might actually need to
                # be removed -- ltrim retains at least one operation
                p.lpop(self.doc_id + ":pending")

            # commit a new minimal revision
            p.set(self.doc_id + ":minimal",
                  self._format_minimal(pending_rev, pending_ts, pending_user_id, content))
            p.execute()

        return new_min_rev


class MemoryBackend(object):
    """Simple backend that saves all operations in the server's memory. This
    causes the processe's heap to grow indefinitely.
    """

    def __init__(self, latest="", operations=[]):
        self.latest = latest
        self.operations = operations[:]
        self.last_operation = {}

    def add_client(self, user_id, min_rev=-1):
        self.last_operation[user_id] = min_rev

    def remove_client(self, user_id):
        del self.last_operation[user_id]

    def get_clients(self):
        return self.last_operation.keys()

    def save_operation(self, user_id, operation):
        """Save an operation in the database."""
        self.latest = operation(self.latest)
        self.last_operation[user_id] = len(self.operations)
        self.operations.append(operation)

    def get_operations(self, start, end=None):
        """Return operations in a given range."""
        return self.operations[start:end]

    def get_last_revision_from_user(self, user_id):
        """Return the revision number of the last operation from a given user."""
        return self.last_operation.get(user_id, None)

    def get_latest(self):
        return len(self.operations), self.latest


class ServerDocument(object):
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
        if last_by_user >= revision:
            return

        concurrent_operations = self.backend.get_operations(revision)
        for concurrent_operation in concurrent_operations:
            operation, _ = operation.transform(operation, concurrent_operation)

        self.backend.save_operation(user_id, operation)
        return operation
