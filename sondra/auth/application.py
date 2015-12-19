import datetime
import bcrypt
import jwt

from sondra.application import Application
from sondra.decorators import expose
from .collections import Users, UserCredentials, LoggedInUsers, Roles
from .decorators import authenticated_method


class Auth(Application):

    db = 'auth'
    collections = (
        Roles,
        Users,
        UserCredentials,
        LoggedInUsers
    )

    def get_expiration_claims(self):
        now = datetime.datetime.now()

        claims = {
            'nbf': int(now.timestamp()),
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
            user = self['users'][username]
        except KeyError:
            self.log.warning("Failed login attempt by nonexistent user: {0}".format(username))
            raise PermissionError("Login not valid")

        credentials = self['user-credentials'][user.url]
        hashed_real_password = credentials['password'].encode('utf-8')
        hashed_given_password = bcrypt.hashpw(password.encode('utf-8'), credentials['salt'].encode('utf-8'))
        if hashed_real_password == hashed_given_password:
            return self.issue(username, credentials)
        else:
            self.log.warning("Failed login attempt by registered user: {0}".format(username))
            raise PermissionError("Login not valid")

    @authenticated_method
    @expose
    def logout(self, token: str) -> None:
        del self['logged-in-users'][token]

    @authenticated_method
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
