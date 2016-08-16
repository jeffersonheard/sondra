from sondra.auth import Auth, Credentials, User, Role
import pytest

from sondra import suite
from sondra.api.ref import Reference

class ConcreteSuite(suite.Suite):
    url = "http://localhost:5000/api"


s = ConcreteSuite()

auth = Auth(s)

s.clear_databases()

@pytest.fixture
def calvin(request):
    r = s['auth']['roles'].create({
        "title": "Calvin Role",
        "description": "Calvin Role Description",
        "permissions": [
            {
                "application": "auth",
                "collection": "roles",
                "allowed": ["read","write","delete"]
            }
        ]
    })
    u = s['auth']['users'].create_user(
        username='calvin',
        password='password',
        email='user@nowhere.com',
        family_name='Powers',
        given_name='Calvin',
        names=['S'],
        roles=[r]
    )
    assert u == 'http://localhost:5000/api/auth/users/calvin'

    def teardown():
        Reference(s, u).value.delete()
        r.delete()

    request.addfinalizer(teardown)

    return u

@pytest.fixture
def local_calvin(calvin):
    return s['auth']['users']['calvin']


@pytest.fixture
def role(request):
    r = s['auth']['roles'].create({
        "title": "Test Role",
        "description": "Test Role Description",
        "permissions": [
            {
                "application": "auth",
                "collection": "roles",
                "allowed": ["read","write","delete"]
            }
        ]
    })
    def teardown():
        r.delete()
    request.addfinalizer(teardown)

    return s['auth']['roles']['test-role']


def test_credentials(local_calvin):
    creds = s['auth']['user-credentials'][local_calvin]
    assert isinstance(creds, Credentials)
    assert creds['password'] != 'password'
    assert creds['salt']
    assert creds['secret']
    assert creds['salt'] != creds['secret']


def test_role(role):
    assert role['slug'] == 'test-role'
    assert 'test-role' in s['auth']['roles']
    assert (role.authorizes(s['auth']['roles'], 'write') == True)
    assert (role.authorizes(s['auth']['roles'], 'read') == True)


def test_user_role(local_calvin):
    assert isinstance(local_calvin['roles'][0], Role)


def test_login_local(local_calvin):
    # test login
    token = s['auth'].login(local_calvin['username'], 'password')
    assert isinstance(token, str)
    assert '.' in token
    assert isinstance(s['auth'].check(token, user=local_calvin['username']), User)

    # Test logout
    s['auth'].logout(token)
    creds = s['auth']['user-credentials'][local_calvin]
    assert creds['secret'] not in s['auth']['logged-in-users']


def test_renew(local_calvin):
    token = s['auth'].login(local_calvin['username'], 'password')

    # test renew
    try:
        token2 = s['auth'].renew(token)
        assert token2 != token
        print(token2)
        print(token)
        assert s['auth'].check(token2, user='calvin')
        assert s['auth']['logged-in-users'].for_token(token) is None
        assert s['auth']['logged-in-users'].for_token(token2) is not None
    finally:
        s['auth'].logout(token)


