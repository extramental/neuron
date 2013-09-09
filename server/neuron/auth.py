class DummyAuthPolicy(object):
    def authenticate(self, request):
        return "dummy"

    def authorize(self, doc_id):
        return True
