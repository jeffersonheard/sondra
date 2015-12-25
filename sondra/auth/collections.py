import bcrypt
import datetime
import rethinkdb as r

from sondra.auth.decorators import authorized_method, authorization_required, authentication_required
from sondra.expose import expose_method
from sondra.collection import Collection
from .documents import Credentials, Role, User, LoggedInUser


@authorization_required('write','add','delete')
@authentication_required('read')
class Roles(Collection):
    primary_key = 'slug'
    document_class = Role


class UserCredentials(Collection):
    primary_key = 'user'
    document_class = Credentials
    private = True


@authorization_required('write', 'add', 'delete')
@authentication_required('read')
class Users(Collection):
    document_class = User
    primary_key = 'username'

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

    @authorized_method
    @expose_method
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
            active: bool=True,
            roles: list=None,
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

        if not email:
            email = username

        if not username:
            username = email

        if roles:
            for role in roles:
                assert role in self.suite['auth']['roles']

        user_data = {
            "username": username,
            "email": email,
            "emailVerified": False,
            "active": active,
            "locale": locale,
            "created": datetime.datetime.now(),
            "roles": roles
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
        self.validate_password(password)
        salt = bcrypt.gensalt()
        secret = bcrypt.gensalt(16)
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
        self.application['user-credentials'].create({
            'user': user,
            'password': hashed_password.decode('utf-8'),
            'salt': salt.decode('utf-8'),
            'secret': secret.decode('utf-8')
        })

        return user.url


class LoggedInUsers(Collection):
    primary_key = 'secret'
    document_class = LoggedInUser
    indexes = ['token', 'time']

    private = True

    def for_token(self, token):
        return next(self.q(self.table.get_all(token, index='token')))

    def delete_token(self, token):
        self.q(self.table.get_all(token, index='token').delete())

    def delete_expired_tokens(self):
        self.q(
            self.table.filter(r.row['expires'] <= r.now())
        )