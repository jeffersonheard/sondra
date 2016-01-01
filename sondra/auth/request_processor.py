from sondra.api import RequestProcessor
from sondra.suite import Suite
from sondra.document import Document
from sondra.collection import Collection
from sondra.application import Application

class AuthRequestProcessor(RequestProcessor):
    """APIRequest processor makes sure that a user is authorized to perform an operation"""

    def authentication_requirement(self, target, permission):
        """Check the target to see if it or any of its 'parents' require authentication."""
        if isinstance(target, tuple):
            req = getattr(target[1], 'authentication_required', None)
            if req is False:
                return None
            elif req is not None:
                return target[0]  # return the object the method is bound to, as this is what permissions are set upon
            else:
                return self.authentication_requirement(target[0], 'write')  # assume all methods are write-dangerous
        elif permission in getattr(target, 'authentication_required', []):
            return target
        elif isinstance(target, Document):
            return self.authentication_requirement(target.collection, permission)
        elif isinstance(target, Collection):
            return self.authentication_requirement(target.application, permission)
        elif isinstance(target, Application):
            return self.authentication_requirement(target.suite, permission)
        else:
            return None

    def authorization_requirement(self, target, permission):
        """Check the target to see if it or any of its 'parents' require authorization."""
        if isinstance(target, tuple):
            req = getattr(target[1], 'authorization_required', None)
            if req is False:
                return None
            elif req is not None:
                return target[0]  # return the object the method is bound to, as this is what permissions are set upon
            else:
                return self.authorization_requirement(target[0], 'write')  # assume all methods are write-dangerous
        elif permission in getattr(target, 'authorization_required', []):
            return target
        elif isinstance(target, Document):
            return self.authorization_requirement(target.collection, permission)
        elif isinstance(target, Collection):
            return self.authorization_requirement(target.application, permission)
        elif isinstance(target, Application):
            return self.authorization_requirement(target.suite, permission)
        else:
            return None

    def process_api_request(self, request):
        reference = request.reference
        if reference.kind == 'subdocument':
            _, auth_target, _, _ = reference.value
        else:
            auth_target = reference.value

        permission_name = self._get_permission_name(request)
        authentication_target = self.authentication_requirement(auth_target, permission_name)
        authorization_target = self.authorization_requirement(auth_target, permission_name)

        if authentication_target is None:  # authentication is not required
            return request
        if reference.format == 'schema':  # always allow schema calls
            return request
        if reference.format == 'help':  # always allow help calls
            return request

        # Check to see if the user has passed a JWT
        auth_token = request.api_arguments.get('_auth', None)  # if the user passed it as a parameter
        if not auth_token:  # maybe the user passed it as a header
            bearer = request.headers.get('Authorization', None)
            if bearer:
                auth_token = bearer[7:]  # skip "Bearer "

        if auth_token:  # check which user the token belongs to; that is the request's user
            user = request.suite['auth'].check(auth_token)
            request.user = user
        else:
            request.user = None
            user = None

        if ((authentication_target is not None) or (authorization_target is not None)) \
                and (not user):
            print("Permission error!!!!!")
            raise PermissionError("Target {url} requires authentication or authorization, but user is anonymous".format(url=reference.url))
        if user and user['admin']:  # allow the superuser unfettered access
            return request
        if authorization_target is None:  # we've authenticated, that's all we need.
            return request

        for role in user.fetch('roles'):  # check each role. return at the first successful authorization.
            if role.authorizes(authorization_target, permission_name):
                return request
        else:  # we made it through all the user's roles and none authorized access.
            msg = "Permission '{name}' denied for '{user}' accessing '{url}'".format(
                user=user,
                url=reference.url,
                name=permission_name
            )
            request.suite.log.error(msg)
            raise PermissionError(msg)

    def _get_permission_name(self, request):
        if request.reference.kind.endswith('method'):
            return request.reference.value[1].slug
        if request.reference.format in ('help','schema'):
            return 'meta'
        if request.request_method == 'GET':
            return 'read'
        elif request.request_method == 'POST':
            return 'write'
        elif request.request_method == 'PUT':
            return 'write'
        elif request.request_method == 'PATCH':
            return 'write'
        elif request.request_method == 'DELETE':
            return 'write'
        else:
            raise ValueError("request method is not GET, POST, PUT, PATCH, or DELETE")