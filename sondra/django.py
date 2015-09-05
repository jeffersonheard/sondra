from .document import Suite
from .api import APIRequest

def create_api_request(request):
    suite = document.Suite()
    return APIRequest(suite, request.body, request.META, request.method, request.user,
               request.path, request.REQUEST, request.FILES)

