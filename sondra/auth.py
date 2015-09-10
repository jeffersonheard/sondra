from sondra.document import Application, Collection, Document, Time
from sondra.decorators import expose
import datetime
import bcrypt
import jwt
import rethinkdb as r

class Authorization(object):
    def authorize(self, user, permission):
        return True


class JWTAuthentication(object):
    def authenticate(self, user, password):
        pass


class Auth(Application):
    SECONDS_CREDENTIALS_VALID = 3600

    db = 'auth'

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
            str: the username that the token came from.
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
            return decoded['user']
        except KeyError:
            raise PermissionError("Token not present in system")


class Role(Document):
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
                "type": "array",
                "items": {"type": "string"},
                "description": "The permissions this role grants"
            },
            "parents": {
                "type": "array",
                "items": {"type": "string"},
                "description": "References to parent roles"
            }
        }
    }


class Credentials(Document):
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
                'description': "The user's secret key, used in JWT authentication"
            },
            'jwtClaims': {
                'type': 'object',
                'description': "Any additional claims to add to a user's JSON Web Token before encoding."
            }
        }
    }


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
                "type": "string",
                "description": "Roles that have been granted to this user",
            }
        }
    }


class Roles(Collection):
    document_class = Role
    application = Auth



class UserCredentials(Collection):
    document_class = Credentials
    application = Auth
    private = True


class Users(Collection):
    document_class = User
    application = Auth
    primary_key = 'username'
    specials = {
        'created': Time()
    }
    relations = [
        ('credentials', UserCredentials),
        ('roles', Roles)
    ]

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
        if len(password) < 8:
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
            picture: str=None
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
            "active": True,
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
    application = Auth
    document_class = LoggedInUser
    indexes = ['token', 'time']
    specials = {
        "expires": Time()
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