from sondra.document import Application, Collection, Document
from sondra.decorators import expose
import pyjwt

class Authorization(object):
    def authorize(self, user):
        return True

class JWTAuthentication(object):
    def authenticate(self, user, password):
        pass


class Auth(Application):
    db = 'auth'

    @expose
    def login(self, username: str, password: str) -> str:
        pass

    @expose
    def logout(self, token: str) -> None:
        pass

class User(Document):
    schema = {
        'type': 'object',
        'properties': {
            'username': {'type': 'string'},
            'email': {'type': 'string'},
            'names': {'type': 'array', 'items': {'type': 'string'}}
        }
    }

class UserAuth(Document):
    schema = {
        'type': 'object',
        'properties': {
            'user': {'type': 'string'},
            'password': {'type': 'string'},
            'salt': {'type': 'string'},
            'secret_key': {'type': 'string'},
        }
    }