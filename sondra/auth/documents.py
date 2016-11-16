import functools
import operator

from sondra.api.expose import expose_method
from sondra.application import Application
from sondra.auth.decorators import authorized_method
from sondra.collection import Collection
from sondra.document import Document
from sondra.document.processors import SlugPropertyProcessor
from sondra.schema import S


class Role(Document):
    template = '${title}'
    schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "slug": {"type": "string"},
            "description": {"type": "string"},
            "permissions": {"type": "array", "items": {"$ref": "#/definitions/permission"}}
        }
    }
    definitions = {
        "permission": {
            "type": "object",
            "properties": {
                "application": {"type": "string"},
                "collection": {"type": "string"},
                "document": {"type": "string"},
                "allowed": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Should be one of 'read','write','add','delete' or the slugged-name of a "
                                   "(application/collection/document) method"
                }
            }
        }
    }
    processors = [
        SlugPropertyProcessor('title')
    ]

    def authorizes(self, value, perm=None):
        if isinstance(value, Application):
            application = value.slug
            for permission in self.get('permissions', []):
                if permission.get('application', None) == application:
                    return perm in permission['allowed']
            else:
                return False

        elif isinstance(value, Collection):
            collection = value.slug
            application = value.application.slug
            for permission in self.get('permissions', []):
                if permission.get('collection', None) == collection and \
                        permission.get('application', None) == application:
                    return perm in permission['allowed']
            else:
                return False

        elif isinstance(value, Document):
            document = value.slug
            collection = value.collection.slug
            application = value.application.slug
            for permission in self.get('permissions', []):
                if permission.get('collection', None) == collection and \
                        permission.get('application', None) == application and \
                        permission.get('document', None) == document:
                    return perm in permission['allowed']
            else:
                return False

        else:
            return False
                        

class User(Document):
    """A basic, but fairly complete system user record"""
    active_by_default = True
    template = "${username}"

    schema = {
        'type': 'object',
        'required': ['email'],
        'properties': S.props(
            ('username', {
                'title': 'Username',
                'type': 'string',
                'description': 'The user\'s username',
            }),
            ('email', {
                'title': 'Email',
                'type': 'string',
                'description': 'The user\'s email address',
                'format': 'email',
                'pattern': '^[^@]+@[^@]+\.[^@]+$'
            }),
            ('email_verified', {
                'title': 'Email Verified',
                'type': 'boolean',
                'description': 'Whether or not this email address has been verified',
            }),
            ('picture', S.image(description='A URL resource of a photograph')),
            ('family_name', {
                'title': 'Family Name',
                'type': 'string',
                'description': 'The user\'s family name',
            }),
            ('given_name', {
                'title': 'Given Name',
                'type': 'string',
                'description': 'The user\'s family name',
            }),
            ('names', {
                'title': 'Other Names',
                'type': 'array',
                'items': {'type': 'string'},
                'description': 'A list of names that go between the given name and the family name.',
            }),
            ('locale', {
                'title': 'Default Language',
                'type': 'string',
                'description': "The user's locale. Default is en-US",
                'default': 'en-US'
            }),
            ('active', {
                'title': 'Active',
                'type': 'boolean',
                'description': 'Whether or not the user is currently able to log into the system.',
                'default': active_by_default
            }),
            ('admin', {
                'title': 'Administrator',
                'type': 'boolean',
                'description': 'If true, this user can access all methods of all APIs.',
                'default': False
            }),
            ('created', {
                'title': 'Created',
                'format': 'date-time',
                'type': 'string',
                'description': 'The timestamp this user was created',
            }),
            ("roles", {
                'title': 'Roles',
                "type": "array",
                "items": S.fk('api', 'auth', 'roles'),
                "description": "Roles that have been granted to this user",
            }),
            ("dob", {
                "title": "Date of Birth",
                "type": "string",
                "format": "date-time",
                "description": "The user's birthday"
            })
        )
    }

    @expose_method
    def permissions(self) -> [dict]:
        return functools.reduce(operator.add, [role['permissions'] for role in self.fetch('roles')], [])

    @expose_method
    def confirm_email(self, confirmation_code: str) -> bool:
        confirmed = self.application['credentials'][self.url]['confirmation_code'] == confirmation_code
        self['confirmed'] = self['confirmed'] or confirmed
        self.save()
        return self['confirmed']

    @authorized_method
    @expose_method
    def send_confirmation_email(self, _user=None) -> None:
        raise NotImplemented()

    def __str__(self):
        return self['username']


class Credentials(Document):
    """A set of credentials for a user"""
    schema = {
        'type': 'object',
        'required': ['password', 'salt', 'secret'],
        'properties': {
            'user': S.fk('api', 'auth', 'users'),
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
            },
            'confirmation_code': {
                'type': 'string',
                'description': "A generated code that confirms a user's email"
            }
        }
    }


class LoggedInUser(Document):
    schema = {
        "type": "object",
        "properties": {
            'token': {"type": "string"},
            "secret": {"type": "string"},
            "expires": {"type": "string"}
        }
    }

class IssuedToken(Document):
    schema = S.object(
        properties=S.props(
            ('token', S.string()),
            ('user', S.fk('auth','users')),
            ('exp', S.datetime()),
        )
    )
