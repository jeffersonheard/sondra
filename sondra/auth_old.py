from functools import reduce
from sondra.application import Application
from sondra.collection import Collection, DateTime
from sondra.document import Document
from sondra.decorators import expose
import datetime
import bcrypt
import jwt
import rethinkdb as r
from sondra.api import RequestProcessor


def _has_permission(perm, permission_tree, keys, default=False):
    """Descends the permission tree looking for the finest grained permission."""
    p = default
    for k in keys:
        new_p = permission_tree['$p'].get(perm, None)
        if new_p is not None:
            p = new_p
        permission_tree = permission_tree.get(k, None)
        if permission_tree is None:
            break

    return p


class AuthorizableDocument(Document):
    anonymous_read = None
    anonymous_write = None
    anonymous_update = None
    anonymous_delete = None
    anonymous_help = None
    anonymous_schema = None
    check_document_permissions = False
    
    def can_read(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            if self.anonymous_read is None:
                return self.collection.can_read(user)
            else:
                return self.anonymous_read
        elif not self.check_document_permissions:
            return self.collection.can_read(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_read = None
            if my_permissions:
                user_can_read = my_permissions['_p'].get('read', None)
            
            if user_can_read is None:
                user_can_read = self.collection.can_read(user)
            
            return user_can_read is True
            
    def can_write(self, user):
        if self.anonymous_write is True:
            return True
        elif user is None:
            if self.anonymous_write is None:
                return self.collection.can_write(user)
            else:
                return self.anonymous_write
        elif not self.check_document_permissions:
            return self.collection.can_write(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_write = None
            if my_permissions:
                user_can_write = my_permissions['_p'].get('add', None)
            
            if user_can_write is None:
                user_can_write = self.collection.can_write(user)
            
            return user_can_write is True
            
    def can_update(self, user):
        if self.anonymous_update is True:
            return True
        elif user is None:
            if self.anonymous_update is None:
                return self.collection.can_update(user)
            else:
                return self.anonymous_update
        elif not self.check_document_permissions:
            return self.collection.can_update(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_update = None
            if my_permissions:
                user_can_update = my_permissions['_p'].get('write', None)
            
            if user_can_update is None:
                user_can_update = self.collection.can_update(user)
            
            return user_can_update is True
        
    def can_delete(self, user):
        if self.anonymous_delete is True:
            return True
        elif user is None:
            if self.anonymous_delete is None:
                return self.collection.can_delete(user)
            else:
                return self.anonymous_delete
        elif not self.check_document_permissions:
            return self.collection.can_delete(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_delete = None
            if my_permissions:
                user_can_delete = my_permissions['_p'].get('delete', None)
            
            if user_can_delete is None:
                user_can_delete = self.collection.can_delete(user)
            
            return user_can_delete is True
            
    def can_view_help(self, user):
        if self.anonymous_help is True:
            return True
        elif user is None:
            if self.anonymous_help is None:
                return self.collection.can_help(user)
            else:
                return self.anonymous_help
        elif not self.check_document_permissions:
            return self.collection.can_help(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_help = None
            if my_permissions:
                user_can_help = my_permissions['_p'].get('help', None)
            
            if user_can_help is None:
                user_can_help = self.collection.can_help(user)
            
            return user_can_help is True
            
    def can_view_schema(self, user):
        if self.anonymous_schema is True:
            return True
        elif user is None:
            if self.anonymous_schema is None:
                return self.collection.can_schema(user)
            else:
                return self.anonymous_schema
        elif not self.check_document_permissions:
            return self.collection.can_schema(user)
        else:
            if user.get('admin', False):
                return True
            
            app = self.application.slug
            coll = self.collection.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(coll, {})
            my_permissions = coll_permissions.get(self.id, None)
            
            user_can_schema = None
            if my_permissions:
                user_can_schema = my_permissions['_p'].get('schema', None)
            
            if user_can_schema is None:
                user_can_schema = self.collection.can_schema(user)
            
            return user_can_schema is True
            

class AuthorizableCollection(Collection):
    anonymous_read = None
    anonymous_write = None
    anonymous_update = None
    anonymous_delete = None
    anonymous_help = None
    anonymous_schema = None

    def can_read(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            if self.anonymous_read is None:
                return self.application.can_read(user)
            else:
                return self.anonymous_read
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_read = None
            if coll_permissions:
                user_can_read = coll_permissions['_p'].get('read', None)

            if user_can_read is None:
                user_can_read = self.application.can_read(user)

            return user_can_read is True

    def can_write(self, user):
        if self.anonymous_write is True:
            return True
        elif user is None:
            if self.anonymous_write is None:
                return self.application.can_write(user)
            else:
                return self.anonymous_write
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_write = None
            if coll_permissions:
                user_can_write = coll_permissions['_p'].get('add', None)

            if user_can_write is None:
                user_can_write = self.application.can_write(user)

            return user_can_write is True

    def can_update(self, user):
        if self.anonymous_update is True:
            return True
        elif user is None:
            if self.anonymous_update is None:
                return self.application.can_update(user)
            else:
                return self.anonymous_update
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_update = None
            if coll_permissions:
                user_can_update = coll_permissions['_p'].get('write', None)

            if user_can_update is None:
                user_can_update = self.application.can_update(user)

            return user_can_update is True

    def can_delete(self, user):
        if self.anonymous_delete is True:
            return True
        elif user is None:
            if self.anonymous_delete is None:
                return self.application.can_delete(user)
            else:
                return self.anonymous_delete
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_delete = None
            if coll_permissions:
                user_can_delete = coll_permissions['_p'].get('delete', None)

            if user_can_delete is None:
                user_can_delete = self.application.can_delete(user)

            return user_can_delete is True

    def can_view_help(self, user):
        if self.anonymous_help is True:
            return True
        elif user is None:
            if self.anonymous_help is None:
                return self.application.can_view_help(user)
            else:
                return self.anonymous_help
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_help = None
            if coll_permissions:
                user_can_help = coll_permissions['_p'].get('help', None)

            if user_can_help is None:
                user_can_help = self.application.can_view_help(user)

            return user_can_help is True

    def can_view_schema(self, user):
        if self.anonymous_schema is True:
            return True
        elif user is None:
            if self.anonymous_schema is None:
                return self.application.can_view_schema(user)
            else:
                return self.anonymous_schema
        else:
            if user.get('admin', False):
                return True

            app = self.application.slug
            permission_tree = user.permissions()
            app_permissions = permission_tree.get(app, {})
            coll_permissions = app_permissions.get(self.slug, None)

            user_can_schema = None
            if coll_permissions:
                user_can_schema = coll_permissions['_p'].get('schema', None)

            if user_can_schema is None:
                user_can_schema = self.application.can_view_schema(user)

            return user_can_schema is True


class AuthorizableApplication(Application):
    anonymous_read = True
    anonymous_write = False
    anonymous_update = False
    anonymous_delete = False
    anonymous_help = True
    anonymous_schema = False

    def can_read(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            return self.anonymous_read is True
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_read = None
            if app_permissions:
                user_can_read = app_permissions['_p'].get('read', None)

            return user_can_read is True

    def can_write(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            return self.anonymous_write is True
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_read = None
            if app_permissions:
                user_can_read = app_permissions['_p'].get('write', None)

            return user_can_read is True
        
    def can_update(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            return self.anonymous_update is True
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_read = None
            if app_permissions:
                user_can_read = app_permissions['_p'].get('add', None)

            return user_can_read is True
        
    def can_delete(self, user):
        if self.anonymous_read is True:
            return True
        elif user is None:
            return self.anonymous_delete is True
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_read = None
            if app_permissions:
                user_can_read = app_permissions['_p'].get('delete', None)

            return user_can_read is True

    def can_view_help(self, user):
        if self.anonymous_help is True:
            return True
        elif user is None:
            return self.anonymous_help is not False
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_help = None
            if app_permissions:
                user_can_help = app_permissions['_p'].get('help', None)

            return user_can_help is True

    def can_view_schema(self, user):
        if self.anonymous_schema is True:
            return True
        elif user is None:
            return self.anonymous_help is not False
        else:
            if user.get('admin', False):
                return True

            permission_tree = user.permissions()
            app_permissions = permission_tree.get(self.slug, None)
            
            user_can_schema = None
            if app_permissions:
                user_can_schema = app_permissions['_p'].get('schema', None)

            return user_can_schema is True


class AuthRequestProcessor(RequestProcessor):
    """APIRequest processor makes sure that a user is authorized to perform an operation"""

    def process_api_request(self, request):
        auth_token = request.api_arguments.get('_auth', None)
        if not auth_token:
            bearer = request.headers.get('Authorization', None)
            if bearer:
                auth_token = bearer[7:]  # skip "Bearer "
        if auth_token:
            user = request.suite['auth'].check(auth_token)
        else:
            user = None

        if request.reference.kind == 'subdocument':
            _, auth_target, _, _ = request.reference.value
        else:
            auth_target = request.reference.value

        permission_name = self._get_permission_name(request)

        if permission_name == 'read':
            authorized =  auth_target.can_read(user)
        elif permission_name == 'write':
            authorized =  auth_target.can_write(user)
        elif permission_name == 'update':
            authorized =  auth_target.can_update(user)
        elif permission_name == 'delete':
            authorized =  auth_target.can_delete(user)
        elif permission_name == 'help':
            authorized =  auth_target.can_view_help(user)
        else:  # permission_name == 'schema':
            authorized =  auth_target.can_view_schema(user)

        if not authorized:
            msg = "Permission '{name}' denied for '{user}' accessing '{url}'".format(
                user=user,
                url=request.reference.url,
                name=permission_name
            )
            request.suite.log.error(msg)
            raise PermissionError(msg)
        else:
            return request

    def _get_permission_name(self, request):
        if request.kind.endswith('method'):
            return request.reference.value.slug
        elif request.format == 'help':
            return 'help'
        elif request.format == 'schema':
            return 'schema'
        elif request.method == 'GET':
            return 'read'
        elif request.method == 'POST':
            return 'write'
        elif request.method == 'PUT':
            return 'update'
        elif request.method == 'PATCH':
            return 'update'
        elif request.method == 'DELETE':
            return 'delete'


class Role(AuthorizableDocument):
    """A role is a group of permissions, which are strings. Each role may have
    one or more "parent" roles, whose permissions it inherits at a minimum.
    """
    schema = {
        "type": "object",
        "required": ['name','permissions'],
        'properties': {
            "name": {
                "type": "string",
                "description": "The name of the role"
            },
            "permissions": {
                "type": "object",
                "description": "The hierarchy of permissions this role grants"
            },
            "includes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "References to additional roles granted by this role"
            }
        }
    }

    @expose
    def grant(self, application, collection=None,  document=None, method=None, action=None) -> None:
        app = application.slug
        perms = self['permissions']
        if app not in perms:
            perms[app] = { '_p': {}}
        perms = perms[app]

        if collection:
            coll = collection.slug
            if coll not in perms:
                perms[coll] = { '_p': {}}
            perms = perms[coll]

            if document:
                doc = document.slug
                if doc not in perms:
                    perms[doc] = { '_p': {}}
                perms = perms[doc]

        if method:
            perms['_p'][method] = True
        elif action:
            perms['_p'][action] = True
        else:
            raise Exception("Must grant/revoke/inherit a method or action")

        self.save()

    @expose
    def revoke(self, application=None, collection=None, document=None, method=None, action=None) -> None:
        app = application.slug
        perms = self['permissions']
        if app not in perms:
            perms[app] = { '_p': {}}
        perms = perms[app]

        if collection:
            coll = collection.slug
            if coll not in perms:
                perms[coll] = { '_p': {}}
            perms = perms[coll]

            if document:
                doc = document.slug
                if doc not in perms:
                    perms[doc] = { '_p': {}}
                perms = perms[doc]

        if method:
            perms['_p'][method] = False
        elif action:
            perms['_p'][action] = False
        else:
            raise Exception("Must grant/revoke/inherit a method or action")

        self.save()

    @expose
    def inherit(self, application=None, collection=None, document=None, method=None, action=None) -> None:
        """
        Cause a role to inherit a permission from the object up the URL tree. This effectively restores the
         default permission to this object.
        """

        app = application.slug
        perms = self['permissions']
        if app not in perms:
            perms[app] = { '_p': {}}
        perms = perms[app]

        if collection:
            coll = collection.slug
            if coll not in perms:
                perms[coll] = { '_p': {}}
            perms = perms[coll]

            if document:
                doc = document.slug
                if doc not in perms:
                    perms[doc] = { '_p': {}}
                perms = perms[doc]

        if method:
            if method in perms['_p']:
                del perms['_p'][method]
        elif action:
            if action in perms['_p']:
                del perms['_p'][action]
        else:
            raise Exception("Must grant/revoke/inherit a method or action")

        self.save()


class Credentials(AuthorizableDocument):
    """A set of credentials for a user"""
    schema = {
        'type': 'object',
        'required': ['password','salt','secret'],
        'properties': {
            'password': {
                'type': 'string',
                'description': "The user's (encrypted) password."
            },
            'salt': {
                'type': 'string',
                'description': "The salt applied to the password"
            },
            'secret': {
                'type': 'string',
                'description': "The user's secret key, used in JWT auth"
            },
            'jwtClaims': {
                'type': 'object',
                'description': "Any additional claims to add to a user's JSON Web Token before encoding."
            }
        }
    }


def _merge(destination, source):
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            _merge(node, value)
        else:
            destination[key] = value
    return destination


class User(AuthorizableDocument):
    """A basic, but fairly complete system user record"""
    schema = {
        'type': 'object',
        'required': ['email'],
        'properties': {
            'username': {
                'type': 'string',
                'description': 'The user\'s username',
            },
            'email': {
                'type': 'string',
                'description': 'The user\'s email address',
            },
            'credentials': {
                'type': 'string',
                'description': 'A reference to the Credentials document associated with this User',
            },
            'emailVerified': {
                'type': 'boolean',
                'description': 'Whether or not this email address has been verified',
            },
            'picture': {
                'type': 'string',
                'description': 'A URL resource of a photograph',
            },
            'familyName': {
                'type': 'string',
                'description': 'The user\'s family name',
            },
            'givenName': {
                'type': 'string',
                'description': 'The user\'s family name',
            },
            'names': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': 'A list of names that go between the given name and the family name.',
            },
            'locale': {
                'type': 'string',
                'description': "The user's locale. Default is en-US",
                'default': 'en-US'
            },
            'active': {
                'type': 'boolean',
                'description': 'Whether or not the user is currently able to log into the system.',
                'default': True
            },
            'admin': {
                'type': 'boolean',
                'description': 'If true, this user can access all methods of all APIs.',
                'default': False
            },
            'created': {
                'type': 'string',
                'description': 'The timestamp this user was created',
            },
            "roles": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Roles that have been granted to this user",
            }
        }
    }

    def __init__(self, obj, collection=None, parent=None):
        super(User, self).__init__(obj, collection, parent)
        self._perms = None

    @expose
    def permissions(self) -> [str]:
        if not self._perms:
            self.dereference()
            self._perms = reduce(_merge, (r['permissions'] for r in self['roles']), {})
        return self._perms

    def __str__(self):
        return self['username']


class Roles(AuthorizableCollection):
    primary_key = 'name'
    document_class = Role
    relations = [
        ('includes', 'self')
    ]


class UserCredentials(Collection):
    document_class = Credentials
    private = True


class Users(AuthorizableCollection):
    document_class = User
    primary_key = 'username'
    specials = {
        'created': DateTime()
    }
    relations = [
        ('credentials', UserCredentials),
        ('roles', Roles)
    ]

    def __init__(self, application):
        super(Users, self).__init__(application)

        # if '__anonymous__' not in self:
        #     self.create_user('__anonymous__', '', '', active=False)
        #
        # self._anonymous_user = self['__anonymous__']
        #
    @property
    def anonymous(self):
        return None  # self._anonymous_user

    def validate_password(self, password):
        """Validate that the desired password is strong enough to use.

        Override this in a subclass if you want stronger controls on the password. This version
        of the function only makes sure that the password has a minimum length of 8.

        Args:
            password (str): The password to use

        Returns:
            None

        Raises:
            ValueError if the password doesn't pass muster.
        """
        if len(password) < 6:
            raise ValueError("Password too short")

    @expose
    def create_user(
            self,
            username: str,
            password: str,
            email: str,
            locale: str='en-US',
            familyName: str=None,
            givenName: str=None,
            names: list=None,
            picture: str=None,
            active: bool=True
    ) -> str:
        """Create a new user

        Args:
            username (str): The username to use. Can be blank. If blank the username is the email.
            password (str): The password to use.
            email (str): The email address for the user. Should be unique
            locale (str="en-US"): The name of the locale for the user
            familyName (str): The user's family name
            givenName (str): The user's given name
            names (str): The user's middle names
            picture (url as str): A pointer to a resource that is the user's picture.
            active (bool): Default true. Whether or not the user is allowed to log in.

        Returns:
            str: The url of the new user object.

        Raises:
            KeyError if the user already exists.
            ValueError if the user's password does not pass muster.
        """
        self.validate_password(password)
        salt = bcrypt.gensalt()
        secret = bcrypt.gensalt(16)
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
        cred = self.application['user-credentials'].create({
            'password': hashed_password.decode('utf-8'),
            'salt': salt.decode('utf-8'),
            'secret': secret.decode('utf-8')
        })
        assert cred
        assert cred.id

        if not email:
            email = username

        if not username:
            username = email

        user_data = {
            "username": username,
            "email": email,
            "credentials": cred,
            "emailVerified": False,
            "active": active,
            "locale": locale,
            "created": datetime.datetime.now()
        }

        if familyName:
            user_data['familyName'] = familyName

        if givenName:
            user_data['givenName'] = givenName

        if names:
            user_data['names'] = names

        if picture:
            user_data['picture'] = picture

        user = self.create(user_data)
        return user.url


class LoggedInUser(Document):
    schema = {
        "type": "object",
        "properties": {
            'token': {"type": "string"},
            "secret": {"type": "string"},
            "expires": {"type": "string"}
        }
    }


class LoggedInUsers(Collection):
    primary_key = 'secret'
    document_class = LoggedInUser
    indexes = ['token', 'time']
    specials = {
        "expires": DateTime()
    }
    private = True

    def for_token(self, token):
        return next(self.q(self.table.get_all(token, index='token')))

    def delete_token(self, token):
        self.q(self.table.get_all(token, index='token').delete())

    def delete_expired_tokens(self):
        self.q(
            self.table.filter(r.row['expires'] <= r.now())
        )

class Auth(AuthorizableApplication):
    SECONDS_CREDENTIALS_VALID = 3600

    db = 'auth'
    collections = (
        Users,
        UserCredentials,
        LoggedInUsers,
        Roles
    )

    def get_expiration_claims(self):
        now = datetime.datetime.now()
        expires_at = now + datetime.timedelta(seconds=Auth.SECONDS_CREDENTIALS_VALID)

        claims = {
            'nbf': int(now.timestamp()),
            'exp': int(expires_at.timestamp())
        }
        return claims

    @expose
    def login(self, username: str, password: str) -> str:
        """Log the user in and get a JWT (JSON Web Token).

        Args:
            username (str): The username
            password (str): The password

        Returns:
            str: A JWT String.

        Raises:
            PermissionError: if the password is invalid or the user does not exist
        """
        try:
            user = self['users'][username].dereference()
        except KeyError:
            self.log.warning("Failed login attempt by fake user: {0}".format(username))
            raise PermissionError("Login not valid")

        credentials = user['credentials']
        hashed_real_password = credentials['password'].encode('utf-8')
        hashed_given_password = bcrypt.hashpw(password.encode('utf-8'), credentials['salt'].encode('utf-8'))
        if hashed_real_password == hashed_given_password:
            return self.issue(username, credentials)
        else:
            self.log.warning("Failed login attempt by registered user: {0}".format(username))
            raise PermissionError("Login not valid")

    @expose
    def logout(self, token: str) -> None:
        del self['logged-in-users'][token]

    @expose
    def renew(self, token: str) -> str:
        """Renew a currently logged in user's token.

        Args:
            token (str): A JSON Web Token (JWT) that is currently valid

        Returns:
            str: A JWT String.

        Raises:
            PermissionError: if the current token is not the user's valid token.
        """

        logged_in_user = self['logged-in-users'].for_token(token)
        secret = logged_in_user['secret'].encode('utf-8')
        claims = jwt.decode(token.encode('utf-8'), secret, issuer=self.url, verify=True)
        claims.update(self.get_expiration_claims())  # make sure this token expires
        token = jwt.encode(claims, secret).decode('utf-8')
        logged_in_user['expires'] = claims['exp']
        logged_in_user['token'] = token
        logged_in_user.save(conflict='replace')
        return token

    def issue(self, username, credentials):
        """Issue a JWT for the given user.

        Args:
            username (str): The username to issue the ticket to.
            credentials (Credentials): The user's credentials object

        Returns:
            str: A JWT String.

        """
        claims = {
            'iss': self.url,
            'user': username
        }
        claims.update(self.get_expiration_claims())  # make sure this token expires
        if 'extraClaims' in credentials:
            claims.update(credentials['extraClaims'])
        token = jwt.encode(claims, credentials['secret']).decode('utf-8')
        self['logged-in-users'].save({
            "token": token,
            "secret": credentials['secret'],
            "expires": claims['exp']
        }, conflict='replace')
        return token


    def check(self, token, **claims):
        """Check a user's JWT for validity and against any extra claims.

        Args:
            token (str): the JWT token to check against.
            **claims: a dictionary of extra claims to check
        Returns:
            User: the user the token came from.
        Raisees:
            DecodeError: if the JWT token is out of date, not issued by this authority, or otherwise invalid.
            PermissionError: if a claim is not present, or if claims differ.
        """

        try:
            logged_in_user = self['logged-in-users'].for_token(token)
            secret = logged_in_user['secret'].encode('utf-8')
            decoded = jwt.decode(token.encode('utf-8'), secret, issuer=self.url, verify=True)
            for name, value in claims.items():
                if name not in decoded:
                    raise PermissionError("Claim not present in {0}: {1}".format(decoded['user'], name))
                elif decoded[name] != value:
                    raise PermissionError("Claims differ for {0}: {1}".format(decoded['user'], name))
            return self['users'][decoded['user']]
        except KeyError:
            raise PermissionError("Token not present in system")
