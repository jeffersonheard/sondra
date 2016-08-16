class RequestProcessor(object):
    """
    Request Processors are arbitrary thunks run before the request is executed. For an example, see the auth application.
    """
    def process_api_request(self, r):
        return r

    def cleanup_after_exception(self, r, e):
        pass

    def __call__(self, r):
        return self.process_api_request(r)