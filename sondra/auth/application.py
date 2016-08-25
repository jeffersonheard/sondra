import datetime

import bcrypt
import jwt

from sondra.api.expose import expose_method, expose_method_explicit
from sondra.application import Application
from sondra.utils import utc_timestamp
from .collections import Users, UserCredentials, LoggedInUsers, Roles, IssuedTokens
from .decorators import authenticated_method


class Auth(Application):

    db = 'auth'
    collections = (
        Roles,
        Users,
        UserCredentials,
        LoggedInUsers,
        IssuedTokens
    )

    def __init__(self, suite, name=None, expiration=None, single_login=True, valid_issuers=None, extra_claims=None, validators=None):
        """
        A sample authentication and authorization app that uses JWT.

        Args:
            suite (sondra.suite.Suite: the suite to register the app to
            name (str): the name to register the app as.
            expiration (optional timedelta): The amount of time to
            single_login (boolean = True): Whether we validate just the token, or we also check to make sure that it's in a single-use registry.
            valid_issuers (list or set): A list of valid issuers to validate the issue claim against.
            extra_claims (optional dict): A dict of claim names to functions that accept a single argument, the user record and return claim content.
            validators (optional list): A list of functions that accept a decoded token and raise an error if the claims aren't verified. The error is passed through.
        """
        super(Auth, self).__init__(suite, name)
        self.expiration = expiration
        self.single_login = single_login
        self.extra_claims = extra_claims or {}
        self.validators = validators or ()
        self.valid_issuers = {self.url}
        if valid_issuers:
            self.valid_issuers.update(set(valid_issuers))

    def get_expiration_claims(self):
        now = utc_timestamp()

        claims = {
            'iat': int(now.timestamp()),
            'nbf': int(now.timestamp()),
        }
        if self.expiration:
            later = now + self.expiration
            claims['exp'] = int(later.timestamp())
        return claims

    @expose_method_explicit(
        request_schema={
            "type": "object",
            "properties": {
                "username": {"type": "string"},
                "password": {"type": "string"}}},
        response_schema={
            "type": "object",
            "properties":
                {"_": {"type": "string"}}},
        side_effects=True,
        title='Login'
    )
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
        if username not in self['users']:
            self.log.warning("Failed login attempt by nonexistent user: {0}".format(username))
            raise PermissionError("Login not valid")

        credentials = self['user-credentials'][username]
        hashed_real_password = credentials['password'].encode('utf-8')
        hashed_given_password = bcrypt.hashpw(password.encode('utf-8'), credentials['salt'].encode('utf-8'))
        if hashed_real_password == hashed_given_password:
            if credentials['secret'] in self['logged-in-users']:
                self['logged-in-users'][credentials['secret']].delete()

            return self.issue(username, credentials)
        else:
            self.log.warning("Failed login attempt by registered user: {0}".format(username))
            raise PermissionError("Login not valid")

    @authenticated_method
    @expose_method
    def logout(self, token: str, _user=None) -> bool:
        u = self['logged-in-users'].for_token(token)
        if u:
            u.delete()
            return True
        else:
            return False

    @authenticated_method
    @expose_method
    def renew(self, token: str, _user=None) -> str:
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
        claims['iat'] = datetime.datetime.now().timestamp()
        claims.update(self.get_expiration_claims())  # make sure this token expires
        token = jwt.encode(claims, secret).decode('utf-8')

        if 'expires' in logged_in_user:
            del logged_in_user['expires']
        if 'exp' in claims:
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
            'user': username,
        }
        if self.extra_claims:
            user = self['users'][username]
            for claim_name, claim_function in self.extra_claims.items():
                claims[claim_name] = claim_function(user)

        if 'extraClaims' in credentials:
            claims.update(credentials['extraClaims'])

        claims.update(self.get_expiration_claims())  # make sure this token expires and that extra claims can't override it.
        token = jwt.encode(claims, credentials['secret']).decode('utf-8')

        if self.single_login:
            logged_in_user = self['logged-in-users'].doc({
                "token": token,
                "secret": credentials['secret']
            })

            if 'exp' in claims:
                logged_in_user['exp'] = claims['exp']

            self['logged-in-users'].save(logged_in_user, conflict='replace')
        else:
            issued_token = {
                'user': username,
                'token': token,
            }
            if 'exp' in claims:
                issued_token['exp'] = datetime.datetime.utcfromtimestamp(claims['exp'])

            self['issued-tokens'].create(issued_token)
        return token

    def check(self, token, **claims):
        """Check a user's JWT for validity and against any extra claims.

        Args:
            token (str): the JWT token to check against.
            **claims: a dictionary of extra claims to check
        Returns:
            User: the decoded auth token.
        Raisees:
            DecodeError: if the JWT token is out of date, not issued by this authority, or otherwise invalid.
            PermissionError: if a claim is not present, or if claims differ.
        """

        if self.single_login:
            logged_in_user = self['logged-in-users'].for_token(token)
            if logged_in_user is None:
                raise PermissionError("Token not present and single login has been configured by the application owner.")
        else:
            logged_in_user = self['issued-tokens'][token]['user']

        secret = logged_in_user['secret'].encode('utf-8')
        decoded = jwt.decode(token.encode('utf-8'), secret, issuer=self.url, verify=True)
        for name, value in claims.items():
            if name not in decoded:
                raise PermissionError("Claim not present in {0}: {1}".format(decoded['user'], name))
            elif decoded[name] != value:
                raise PermissionError("Claims differ for {0}: {1}".format(decoded['user'], name))
        return self['users'][decoded['user']], decoded  # WAS: self['users'][decoded['user']]


    @expose_method_explicit(
        request_schema={
            "type": "object",
            "required": ["token"],
            "properties": {
                "token": {"type": "string"}
            }},
        response_schema={
            "type": "object",
            "properties":
                {"_": {"type": "boolean"}}},
        side_effects=False,
        title='Check',
        description="Check to make sure the token is held in the token store."
    )
    def verify(self, token):
        try:
            self.check(token)
        except PermissionError as e:
            return False

        return True

