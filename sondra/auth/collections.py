import datetime

import bcrypt
import rethinkdb as r

from sondra.api.expose import expose_method, expose_method_explicit
from sondra.auth.decorators import authorized_method, authorization_required, authentication_required, anonymous_method
from sondra.collection import Collection
from .documents import Credentials, Role, User, LoggedInUser, IssuedToken


@authorization_required('write')
@authentication_required('read')
class Roles(Collection):
    primary_key = 'slug'
    document_class = Role
    autocomplete_props = ('title', 'description')
    template = '${title}'


class UserCredentials(Collection):
    primary_key = 'user'
    document_class = Credentials
    private = True


@authorization_required('write')
class Users(Collection):
    document_class = User
    primary_key = 'username'
    indexes = ['email']
    order_by = ('family_name', 'given_name', 'username')

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

    def user_data(self,
            username: str,
            email: str,
            locale: str='en-US',
            password: str=None,
            family_name: str=None,
            given_name: str=None,
            names: list=None,
            active: bool=True,
            roles: list=None,
            confirmed_email: bool=False
    ) -> str:
        """Create a new user

        Args:
            username (str): The username to use. Can be blank. If blank the username is the email.
            password (str): The password to use.
            email (str): The email address for the user. Should be unique
            locale (str="en-US"): The name of the locale for the user
            family_name (str): The user's family name
            given_name (str): The user's given name
            names (str): The user's middle name
            active (bool): Default true. Whether or not the user is allowed to log in.
            roles (list[roles]): List of role objects or urls. The list of roles a user is granted.
            confirmed_email (bool): Default False. The user has confirmed their email address already.

        Returns:
            str: The url of the new user object.

        Raises:
            KeyError if the user already exists.
            ValueError if the user's password does not pass muster.
        """
        email = email.lower()

        if not password:
            active=False
            password=''

        if not username:
            username = email

        if username in self:
            raise PermissionError("Attempt to create duplicate user " + username)

        user = {
            "username": username,
            "email": email,
            "email_verified": False,
            "active": active if active is not None else self.document_class.active_by_default,
            "locale": locale,
            "created": datetime.datetime.now(),
            "roles": roles or [],
            "confirmed_email": confirmed_email
        }

        if family_name:
            user['family_name'] = family_name

        if given_name:
            user['given_name'] = given_name

        if names:
            user['names'] = names

        credentials = None
        if active and password:
            self.validate_password(password)
            salt = bcrypt.gensalt()
            secret = bcrypt.gensalt(16)
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
            credentials = {
                'password': hashed_password.decode('utf-8'),
                'salt': salt.decode('utf-8'),
                'secret': secret.decode('utf-8')
            }

        return user, credentials

    @authorized_method
    @expose_method_explicit(
        title="Create User",
        side_effects=True,
        request_schema={
            "type": "object",
            "required": ['username', 'email'],
            "description": "Create a new user in the system",
            "properties": {
                "username": {"type": "string", "title": "Username", "description": "The new username"},
                "email": {"type": "string", "title": "email", "description": "The user's email"},
                "locale": {"type": "string", "title": "Locale", "description": "The user's default language setting", "default": "en-US"},  #, "format": "locale"},
                "password": {"type": "string", "title": "Password", "description": "The user's password. Leave blank to have it auto-generated."},
                "family_name": {"type": "string", "title": "Family Name", "description": "The user's password"},
                "given_name": {"type": "string", "title": "Given Name", "description": "The user's password"},
                "names": {"type": "string", "title": "Middle Name(s)", "description": "The user's middle names"},
                "active": {"type": "boolean", "title": "Can Login", "description": "The user can login", "default": True},
                "roles": {"type": "array", "title": "Roles", "items": {"type": "string", "fk": "/auth/roles"}, "description": "The roles to assign to the new user.", "default": []},
                "confirmed_email": {"type": "boolean", "default": False, "title": "Confirmed", "description": "Whether or not the user has confirmed their email address."}
            }
        },
        response_schema={
            "type": "object",
            "properties": {"_": {"type": "string", "description": "The new user's URL."}}
        },
    )
    def create_user(
            self,
            username: str,
            email: str,
            locale: str='en-US',
            password: str=None,
            family_name: str=None,
            given_name: str=None,
            names: list=None,
            active: bool=True,
            roles: list=None,
            confirmed_email: bool=False,
            _user=None
    ) -> str:
        """Create a new user

        Args:
            username (str): The username to use. Can be blank. If blank the username is the email.
            password (str): The password to use.
            email (str): The email address for the user. Should be unique
            locale (str="en-US"): The name of the locale for the user
            family_name (str): The user's family name
            given_name (str): The user's given name
            names (str): The user's middle name
            active (bool): Default true. Whether or not the user is allowed to log in.
            roles (list[roles]): List of role objects or urls. The list of roles a user is granted.
            confirmed_email (bool): Default False. The user has confirmed their email address already.

        Returns:
            str: The url of the new user object.

        Raises:
            KeyError if the user already exists.
            ValueError if the user's password does not pass muster.
        """
        user_record, credentials = self.user_data(
            username=username,
            email=email,
            locale=locale,
            password=password,
            family_name=family_name,
            given_name=given_name,
            names=names,
            active=active,
            roles=roles,
            confirmed_email=confirmed_email,
        )
        user = self.create(user_record)
        if credentials:
            credentials['user'] = username
            self.application['user-credentials'].create(credentials)

        return user.url

    def create_users(self, *users):
        us = []
        cs = []
        for u, c in (self.user_data(**x) for x in users):
            us.append(u)
            if c is not None:
                cs.append(c)
        self.create(us)
        if cs:
            self.application['user-credentials'].create(cs)

    @anonymous_method
    @expose_method
    def signup(
        self,
        username: str,
        password: str,
        email: str,
        locale: str='en-US',
        family_name: str=None,
        given_name: str=None,
        names: list=None
    ) -> bool:
        """Create a new user anonymously. by default the user is inactive and email is not confirmed. No roles can be
        assigned except by an admin

        Args:
            username (str): The username to use. Can be blank. If blank the username is the email.
            password (str): The password to use.
            email (str): The email address for the user. Should be unique
            locale (str="en-US"): The name of the locale for the user
            familyName (str): The user's family name
            givenName (str): The user's given name
            names (str): The user's middle names

        Returns:
            str: The url of the new user object.

        Raises:
            KeyError if the user already exists.
            ValueError if the user's password does not pass muster.
        """
        self.create_user(
            username=username,
            password=password,
            email=email,
            family_name=family_name,
            given_name=given_name,
            names=names,
            active=False
        )
        # self[username].send_confirmation_email()
        return True

    @anonymous_method
    @expose_method
    def by_email(self, email: str) -> 'sondra.auth.documents.User':
        email = email.lower()
        u = self.q(self.table.get_all(email, index='email'))
        try:
            return next(u).url
        except:
            return None


class LoggedInUsers(Collection):
    primary_key = 'secret'
    document_class = LoggedInUser
    indexes = ['token']

    private = True

    def for_token(self, token):
        result = self.table.get_all(token, index='token').run(self.application.connection)
        try:
            return self.document_class(next(result), self, True)
        except StopIteration:
            return None

    def delete_token(self, token):
        self.q(self.table.get_all(token, index='token').delete())

    def delete_expired_tokens(self):
        self.q(
            self.table.filter(r.row['expires'] <= r.now())
        )


class IssuedTokens(Collection):
    primary_key = 'token'
    document_class = IssuedToken
    indexes = ['user', 'exp']

    private = True
