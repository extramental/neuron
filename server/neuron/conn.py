import json
import uuid

from sockjs.tornado import SockJSConnection

from .ot import Server as OTServer
from .ot import RedisTextDocumentBackend
from .ot.text_operation import TextOperation

class Connection(SockJSConnection):
    OP_ERROR = 0
    OP_LOAD = 1
    OP_CONTENT = 2
    OP_OPERATION = 3
    OP_ACK = 4
    OP_CURSOR = 5
    OP_LEFT = 6
    OP_JOIN = 7

    OP_MAP = {
        OP_LOAD: "do_load",
        OP_OPERATION: "do_operation",
        OP_CURSOR: "do_cursor",
        OP_LEFT: "do_left"
    }

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.doc_ids = set([])
        self.conn_id = None
        self.user_id = None

    def get_document(self, doc_id):
        if doc_id not in self.application.docs:
            self.application.docs[doc_id] = OTServer(RedisTextDocumentBackend(self.application.redis, doc_id, self.user_id))
        return self.application.docs[doc_id]

    @property
    def application(self):
        return self.session.server.application

    def on_open(self, request):
        user_id = self.application.auth_policy.authenticate(request)

        if user_id is None:
            self.send(json.dumps([self.OP_ERROR, "could not authenticate"]))
            self.close()
            return

        self.user_id = user_id
        self.conn_id = uuid.uuid4().hex.encode("utf-8")
        self.application.conns[self.conn_id] = self


    def on_message(self, msg):
        payload = json.loads(msg)
        opcode, rest = payload[0], payload[1:]
        getattr(self, self.OP_MAP[opcode])(*rest)

    def do_load(self, doc_id):
        if not self.application.auth_policy.authorize(doc_id):
            self.send(json.dumps([self.OP_ERROR, "not permitted"]))
            return

        doc = self.get_document(doc_id)

        self.doc_ids.add(doc_id)

        doc.backend.set_client(self.conn_id, -1)
        rev, content = doc.backend.get_latest()

        self.send(json.dumps([self.OP_CONTENT, doc_id, rev,
                              {k.decode("utf-8"): {"name": str(self.application.conns[k].user_id)}
                               for k
                               in doc.backend.get_clients()
                               if k != self.conn_id and k in self.application.conns},
                              content]))

        self.broadcast_to_doc(doc_id,
                              [self.OP_JOIN, doc_id, self.conn_id.decode("utf-8"), str(self.user_id)])

    def do_operation(self, doc_id, rev, raw_op):
        doc = self.get_document(doc_id)
        op = doc.receive_operation(self.conn_id, rev, TextOperation.deserialize(raw_op))

        if op is None:
            return

        self.send(json.dumps([self.OP_ACK, doc_id]))

        self.broadcast_to_doc(doc_id,
                              [self.OP_OPERATION, doc_id, op.serialize()])

    def do_cursor(self, doc_id, cursor):
        doc = self.get_document(doc_id)

        if cursor is None:
            doc.backend.remove_client_cursor(self.conn_id)
        else:
            pos, end = cursor.split(",")
            doc.backend.set_client_cursor(self.conn_id, int(pos), int(end))

        self.broadcast_to_doc(doc_id,
                              [self.OP_CURSOR, doc_id, self.conn_id.decode("utf-8"), cursor])

    def do_left(self, doc_id):
        self.doc_ids.remove(doc_id)
        self.get_document(doc_id).backend.remove_client(self.conn_id)
        self.broadcast_to_doc(doc_id,
                              [self.OP_LEFT, doc_id, self.conn_id.decode("utf-8")])

    def broadcast_to_doc(self, doc_id, payload):
        doc = self.get_document(doc_id)

        for conn_id in doc.backend.get_clients():
            if conn_id not in self.application.conns:
                doc.backend.remove_client(conn_id)
                continue
            if conn_id == self.conn_id:
                continue
            self.application.conns[conn_id].send(json.dumps(payload))

    def on_close(self):
        try:
            if self.conn_id is None:
                return

            for doc_id in self.doc_ids:
                payload = [self.OP_LEFT, doc_id, self.conn_id]
                doc = self.get_document(doc_id)

                self.get_document(doc_id).backend.remove_client(self.conn_id)

                self.broadcast_to_doc(doc_id,
                                      [self.OP_LEFT, doc_id, self.conn_id.decode("utf-8")])
        except Exception as e:
            logging.error("Error during on_close:", exc_info=True)
