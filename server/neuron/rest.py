from tornado.web import RequestHandler

from .ot import RedisTextDocumentBackend


class DocumentMetaHandler(RequestHandler):
    def get(self, doc_id):
        doc = self.application.get_document_backend(doc_id)
        rev, content = doc.get_latest()

        self.finish({
            "id": doc_id,
            "latest_rev": rev,
            "latest_content": content
        })


class DocumentRevisionHandler(RequestHandler):
    def get(self, doc_id, doc_rev=None):
        doc = self.application.get_document_backend(doc_id)

        if doc_rev is None:
            doc_rev, _ = doc.get_latest()

        rev, w_ops, content = doc.get_history_operations_to_latest(doc_rev)

        if rev == -1:
            return self.finish({
                "latest_rev": -1,
                "content": ""
            })

        name, ts, _ = w_ops[0]

        for _, _, op in reversed(w_ops[1:]):
            content = op(content)

        self.finish({
            "rev": doc_rev,
            "author_name": name,
            "ts": ts,
            "content": content
        })


class RESTRouter(object):
    def __init__(self, prefix):
        self.prefix = prefix

    @property
    def urls(self):
        return [
            (self.prefix + r"/docs/(?P<doc_id>[^/]+)/?", DocumentMetaHandler),
            (self.prefix + r"/docs/(?P<doc_id>[^/]+)/revs/(?P<doc_rev>\d+)?/", DocumentRevisionHandler),
            (self.prefix + r"/docs/(?P<doc_id>[^/]+)/revs/latest/?", DocumentRevisionHandler)
        ]
