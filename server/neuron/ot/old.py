import time

from . import text

class ServerDocument(object):
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

    NEW_DOCUMENT = make_minimal_rev.__func__(0, 0, 0, "")

    def add_client(self, uid, min_rev=-1):
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

    @staticmethod
    def parse_minimal(raw_minimal):
        rev, ts, uid, content = raw_minimal.split(":", 3)
        return int(rev), int(ts), int(uid), content

    def get_minimal(self):
        """
        Get the minimal revision of the document. By "minimal", it means the
        revision that the slowest client is at.
        """
        raw_minimal = self.redis.get(self.id + ":minimal")
        if raw_minimal is None:
            raw_minimal = self.NEW_DOCUMENT
            self.redis.set(self.id + ":minimal", raw_minimal)
        return self.parse_minimal(raw_minimal)

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

    def get_minimal_and_pending(self):
        """
        Get both the minimal text and pending operations, atomically.
        """
        p = self.redis.pipeline()
        p.get(self.id + ":minimal")
        p.lrange(self.id + ":pending", 0, -1)
        raw_minimal, raw_pending = p.execute()
        if raw_minimal is None:
            raw_minimal = self.NEW_DOCUMENT
            self.redis.set(self.id + ":minimal", raw_minimal)

        return self.parse_minimal(raw_minimal), [self.deserialize_op(raw_op)
                                                 for raw_op in raw_pending]

    def get_latest(self):
        """
        Get the latest revision of the document.
        """
        minimal, pending = self.get_minimal_and_pending()

        rev, ts, uid, content = minimal
        for rev, ts, uid, op in pending:
            content = op(content)

        return rev, ts, uid, content

    def get_pending_revision(self, rev):
        """
        Get a revision that can be constructed from operations in the pending
        list.
        """
        minimal, pending = self.get_minimal_and_pending()

        r, ts, uid, content = minimal

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
            for _ in range(n + 1):
                # i would use ltrim, but all pending ops might actually be
                # removed
                p.lpop(self.id + ":pending")

            # commit a new minimal revision
            p.set(self.id + ":minimal",
                  self.make_minimal_rev(pending_rev, pending_ts, pending_uid, content))
            p.execute()

        return new_min_rev

    def receive_operation(self, rev, uid, op):
        """
        Run an operation on the document.
        """
        last_by_user = self.get_last_uids()[uid]

        if last_by_user >= rev:
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
                         self.make_serialized_op(rev, int(time.time()), uid, op))
        self.add_client(uid, rev)

        return op
