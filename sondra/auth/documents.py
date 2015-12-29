from sondra.application import Application
from sondra.auth.decorators import authorized_method
from sondra.collection import Collection
from sondra.expose import expose_method
from sondra.document import Document, SlugPropertyProcessor, DateTime, Now
from sondra.ref import Reference
from sondra.lazy import fk
import operator
import functools


class Role(Document):
    schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "slug": {"type": "string"},
            "description": {"type": "string"},
            "permissions": {"type": "array", "items": {"ref": "#/definitions/permission"}}
        }
    }
    definitions = {
        "permission":   {
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
    specials = {
        'created': DateTime()
    }

    def authorizes(self, value, perm=None):
        if isinstance(value, Application):
            application = value.slug
            for permission in self['permissions']:
                if permission.get('application', None) == application:
                    return perm in permission['allowed']
            else:
                return False

        elif isinstance(value, Collection):
            collection = value.slug
            application = value.application.slug
            for permission in self['permissions']:
                if permission.get('collection', None) == collection and \
                        permission.get('application', None) == application:
                    return perm in permission['allowed']
            else:
                return False

        elif isinstance(value, Document):
            document = value.slug
            collection = value.collection.slug
            application = value.application.slug
            for permission in self['permissions']:
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
                'format': 'email',
                'pattern': '^(\w+[.|\w])*@(\w+[.])*\w+$'
            },
            'email_verified': {
                'type': 'boolean',
                'description': 'Whether or not this email address has been verified',
            },
            'picture': {
                'type': 'string',
                'description': 'A URL resource of a photograph',
            },
            'family_name': {
                'type': 'string',
                'description': 'The user\'s family name',
            },
            'given_name': {
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
                'default': active_by_default
            },
            'confirmed_email': {
                'type': 'boolean',
                'description': 'Whether or not the user has confirmed their email',
                'default': False
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
                "items": fk('sondra.auth.collections.Roles'),
                "description": "Roles that have been granted to this user",
            }
        }
    }
    specials = {
        "created": DateTime()
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
    def send_confirmation_email(self) -> None:
        raise NotImplemented()

    def __str__(self):
        return self['username']


class Credentials(Document):
    """A set of credentials for a user"""
    schema = {
        'type': 'object',
        'required': ['password','salt','secret'],
        'properties': {
            'user': fk('sondra.auth.collections.Users'),
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
    specials = {
        "expires": DateTime()
    }


