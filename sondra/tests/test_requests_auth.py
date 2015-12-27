import pytest
from datetime import datetime
import json
import requests

from sondra.auth.application import Auth
from sondra.tests import api

BASE_URL = api.ConcreteSuite.url

@pytest.fixture(scope='module')
def suite(request):
    s = api.ConcreteSuite()
    Auth(s)

    for app in s.values():
        for coll in app.values():
            coll.delete()
    return s


@pytest.fixture(scope='module')
def administrator(request, suite):
    suite['auth']['users'].create_user('admin', password='password', email='admin@sondra.github.com')
    u = suite['auth']['users']['admin']
    u['admin'] = True
    u.save()

    def teardown():
        u.delete()

    request.addfinalizer(teardown)
    return u


@pytest.fixture(scope='module')
def basic_user(request, suite):
    suite['auth']['users'].create_user('basic', password='password', email='basic@sondra.github.com')
    u = suite['auth']['users']['basic']
    u.save()

    def teardown():
        u.delete()

    request.addfinalizer(teardown)
    return u


@pytest.fixture(scope='module')
def application_level_user(request, suite):
    suite['auth'].create_user(username='app_level', password='password', email='app_level@sondra.github.com')
    u = suite['auth']['users']['app_level']

    r = suite['auth']['roles'].create({
        "title": "Application-Level Role",
        "description": "Application-Level Authority",
        "permissions": [
            {
                "application": "authorized-application",
                "allowed": ["read", "write", "authorized-method"],
            }
        ]
    })
    u['roles'] = [r]
    u.save()

    def teardown():
        u.delete()

    request.addfinalizer(teardown)
    return u


@pytest.fixture(scope='module')
def collection_level_user(request, suite):
    suite['auth']['users'].create_user('coll_level', password='password', email='coll_level@sondra.github.com')
    u = suite['auth']['users']['coll_level']

    r = suite['auth']['roles'].create({
        "title": "Collection-Level Role",
        "description": "Collection-Level Authority",
        "permissions": [
            {
                "application": "authorized-application",
                "collection": "authorized-collection",
                "allowed": ["read", "write", "authorized-method"],
            }
        ]
    })
    u['roles'] = [r]
    u.save()

    def teardown():
        u.delete()

    request.addfinalizer(teardown)
    return u


def _login(u):
    login_result = requests.post('http://localhost:5000/api/auth.login', data=json.dumps({
        "username": "basic", "password": "password"
    }))

    assert login_result.ok
    wrapper = login_result.json()
    assert '_' in wrapper
    jwt = wrapper['_']
    return jwt


def _logout(jwt):
    logout_result = requests.post(
        'http://localhost:5000/api/auth.logout',
        headers={"Authorization": "Bearer:"+jwt},
        data=json.dumps({"token": jwt}))

    assert logout_result.ok


def _auth_header(jwt):
    return {"Authorization": "Bearer "+jwt}


def test_login_logout(basic_user):
    jwt = _login(basic_user)

    result = requests.post('http://localhost:5000/api/simple-app.authenticated-method', headers=_auth_header(jwt))
    assert result.ok

    _logout(jwt)

    result = requests.post('http://localhost:5000/api/simple-app.authenticated-method', headers=_auth_header(jwt))
    assert not result.ok, "Request succeeded using token that has been logged out"

    result = requests.post('http://localhost:5000/api/auth.login', data=json.dumps({
        "username": "basic", "password": "badpassword"
    }))
    assert not result.ok


def test_renew(basic_user):
    jwt = _login(basic_user)

    result = requests.post('http://localhost:5000/api/auth.renew', data=json.dumps({"token": jwt}), headers=_auth_header(jwt))
    assert result.ok, result.text
    new_jwt = result.json()['_']

    # shouldn't be able to use an expired token
    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents;json', headers=_auth_header(jwt))
    assert not result.ok, result.text

    # should be able to use the new token
    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents;json', headers=_auth_header(new_jwt))
    assert result.ok, result.text

    _logout(basic_user)


def test_signup(suite):
    result = requests.post('http://localhost:5000/api/auth/users.signup', data={
        'username': 'signup',
        'password': 'signuppassword',
        'email': 'signup@sondra.github.com',
        'givenName': "Jefferson",
        'familyName': "Heard"
    })
    assert result.ok

    assert 'signup' in suite['auth']['users']
    u = suite['auth']['users']['signup']
    assert u['active'] is False
    assert u['confirmedEmail'] is False
    u.delete()


def test_anonymous_meta():
    result = requests.get('http://localhost:5000/api/simple-app;schema')
    assert result.ok

    result = requests.get('http://localhost:5000/api/simple-app;help')
    assert result.ok

    result = requests.get('http://localhost:5000/api/authorized-app;schema')
    assert result.ok

    result = requests.get('http://localhost:5000/api/authenticated-app;help')
    assert result.ok


def test_authenticated_collection():
    result = requests.get('http://localhost:5000/api/authenticated-app/simple-documents;json')
    assert result.ok

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents;json')
    assert not result.ok

    result = requests.get('http://localhost:5000/api/authenticated-app/authorized-documents;json')
    assert not result.ok


def test_admin(administrator):
    jwt = _login(administrator)
    headers = _auth_header(jwt)

    result = requests.get('http://localhost:5000/api/authenticated-app/simple-documents;json', headers=headers)
    assert result.ok

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents;json', headers=headers)
    assert result.ok

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents;json', headers=headers)
    assert result.ok

    result = requests.get('http://localhost:5000/api/authorized-app/simple-documents.simple-none-return;json', headers=headers)
    assert result.ok

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents.simple-none-return;json', headers=headers)
    assert result.ok

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents.simple-none-return;json', headers=headers)
    assert result.ok


def test_authenticated_app_method_in_anon_app():
    result = requests.post('http://localhost:5000/api/simple-app.authenticated-method')
    assert not result.ok

    result = requests.post('http://localhost:5000/api/simple-app.authorized-method')
    assert not result.ok


# def test_authenticated_collection_method_in_anon_app():
#     pass
#
#
# def test_exposed_collection_method_in_authenticated_app():
#     pass
#
#
# def test_exposed_app_method_in_authenticated_app():
#     pass
#
#
# def test_exposed_document_method_in_authenticated_app():
#     pass
#
#
# def test_authenticated_collection_read():
#     pass
#
#
# def test_authenticated_collection_write():
#     pass
#
#
# def test_authenticated_document_read():
#     pass
#
#
# def test_authorized_app_method_in_anon_app():
#     pass
#
#
# def test_authorized_collection_method_in_anon_app():
#     pass
#
#
# def test_exposed_collection_method_in_authorized_app():
#     pass
#
#
# def test_exposed_app_method_in_authorized_app():
#     pass
#
#
# def test_exposed_document_method_in_authorized_app():
#     pass
#
#
# def test_authorized_collection_read():
#     pass
#
#
# def test_authorized_collection_write():
#     pass
#
#
# def test_authorized_document_read():
#     pass
#
#
# def test_anonymous_collection_read():
#     pass
#
#
# def test_anonymous_document_read():
#     pass
#
#
# def test_anonymous_document_write_fails():
#     pass
#
#
# def test_anonymous_collection_write_fails():
#     pass
#
#
# def test_anonymous_help():
#     pass
#
#
# def test_anonymous_schema():
#     pass