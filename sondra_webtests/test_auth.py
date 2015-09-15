import docs
from sondra.auth import Auth, UserCredentials, Credentials, Users, User
import pytest

s = docs.ConcreteSuite()

auth = Auth(s)
auth.drop_tables()
auth.create_database()
auth.create_tables()

@pytest.fixture
def calvin(request):
    u = s['auth']['users'].create_user(
        username='calvin',
        password='password',
        email='user@nowhere.com',
        familyName='Powers',
        givenName='Calvin',
        names=['S'],
    )
    assert u == 'http://localhost:5000/api/auth/users/calvin'

    def teardown():
        s['auth']['users']['calvin'].delete()
    request.addfinalizer(teardown)

    return u

@pytest.fixture
def local_calvin(calvin):
    return s['auth']['users']['calvin']


def test_login_local(local_calvin):

    assert local_calvin['username'] == 'calvin'
    local_calvin.dereference()

    creds = local_calvin['credentials']
    assert isinstance(creds, Credentials)
    assert creds
    assert creds['password'] != 'password'
    assert creds['salt']
    assert creds['secret']
    assert creds['salt'] != creds['secret']

    token = s['auth'].login(local_calvin['username'], 'password')
    assert isinstance(token, str)
    assert '.' in token
    assert isinstance(s['auth'].check(token, user='calvin'), User)

    token2 = s['auth'].renew(token)
    assert s['auth'].check(token2, user='calvin')
