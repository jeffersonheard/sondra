from sondra.decorators import expose
from sondra.document import Document, SlugPropertyProcessor
from sondra.ref import Reference
from sondra.lazy import fk


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
    
    def authorizes(self, reference, perm=None):
        if isinstance(reference, str):
            reference = Reference(self.suite, reference)

        if reference.kind == 'application_method':
            application = reference.get_application()
            meth = reference.get_application_method()
            for permission in self['permissions']:
                if permission.get('application', None) == application.slug:
                    return meth.slug in permission['allowed']
            else:
                return False

        elif reference.kind == 'collection_method':
            collection = reference.get_collection()
            meth = reference.get_collection_method()
            for permission in self['permissions']:
                if permission.get('collection', None) == collection.slug and \
                        permission.get('application', None) == collection.application.slug:
                    return meth.slug in permission['allowed']
            else:
                return False

        elif reference.kind == 'document_method':
            doc = reference.get_document()
            meth = reference.get_document_method()
            for permission in self['permissions']:
                if permission.get('collection', None) == doc.collection.slug and \
                        permission.get('application', None) == doc.collection.application.slug and \
                        permission.get('document', None) == doc.id:
                    return meth.slug in permission['allowed']
            else:
                return False

        elif reference.kind == 'application':
            application = reference.get_application()
            for permission in self['permissions']:
                if permission.get('application', None) == application.slug:
                    return perm in permission['allowed']
            else:
                return False

        elif reference.kind == 'collection':
            collection = reference.get_collection()
            for permission in self['permissions']:
                if permission.get('collection', None) == collection.slug and \
                        permission.get('application', None) == collection.application.slug:
                    return perm in permission['allowed']
            else:
                return False

        elif reference.kind == 'document' or reference.kind == 'subdocument':
            doc = reference.get_document()
            for permission in self['permissions']:
                if permission.get('collection', None) == doc.collection.slug and \
                        permission.get('application', None) == doc.collection.application.slug and \
                        permission.get('document', None) == doc.id:
                    return perm in permission['allowed']
            else:
                return False

        else:
            return False
                        

class User(Document):
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
                "items": fk('sondra.auth.collections.Roles'),
                "description": "Roles that have been granted to this user",
            }
        }
    }

    @expose
    def permissions(self) -> [str]:
        pass

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



