from sondra.api import RequestProcessor


class AuthRequestProcessor(RequestProcessor):
    """APIRequest processor makes sure that a user is authorized to perform an operation"""

    def authentication_requirement(self, target, permission):
        if permission in getattr(target, 'authentication_required', []):
            return target
        elif hasattr(target, 'collection'):
            return self.authentication_requirement(target.collection, permission)
        elif hasattr(target, 'application'):
            return self.authentication_requirement(target.application, permission)
        elif hasattr(target, 'suite'):
            return self.authentication_requirement(target.suite, permission)
        else:
            return None

    def authorization_requirement(self, target, permission):
        if permission in getattr(target, 'authentication_required', []):
            return target
        elif hasattr(target, 'collection'):
            return self.authentication_requirement(target.collection, permission)
        elif hasattr(target, 'application'):
            return self.authentication_requirement(target.application, permission)
        elif hasattr(target, 'suite'):
            return self.authentication_requirement(target.suite, permission)
        else:
            return None


    def process_api_request(self, request):
        if request.reference.kind == 'subdocument':
            _, auth_target, _, _ = request.reference.value
        else:
            auth_target = request.reference.value

        permission_name = self._get_permission_name(request)
        authentication_target = self.authentication_requirement(auth_target, permission_name)
        authorization_target = self.authorization_requirement(auth_target, permission_name)

        if authentication_target is None:
            return request
        if request.format == 'schema':
            return request  # always allow schema calls
        if request.format == 'help':
            return request  # always allow help calls

        auth_token = request.api_arguments.get('_auth', None)
        if not auth_token:
            bearer = request.headers.get('Authorization', None)
            if bearer:
                auth_token = bearer[7:]  # skip "Bearer "
        if auth_token:
            user = request.suite['auth'].check(auth_token)
        else:
            user = None

        if user['admin']:
            return request  # allow the superuser unfettered access
        if authorization_target is None:
            return request  # we've authenticated, that's all we need.

        user.dereference()
        for role in user['roles']:
            if role.authorizes(authorization_target, permission_name):
                return request

        else:
            msg = "Permission '{name}' denied for '{user}' accessing '{url}'".format(
                user=user,
                url=request.reference.url,
                name=permission_name
            )
            request.suite.log.error(msg)
            raise PermissionError(msg)


    def _get_permission_name(self, request):
        if request.kind.endswith('method'):
            return request.reference.value.slug
        if request.method == 'GET':
            return 'read'
        elif request.method == 'POST':
            return 'add'
        elif request.method == 'PUT':
            return 'write'
        elif request.method == 'PATCH':
            return 'write'
        elif request.method == 'DELETE':
            return 'delete'