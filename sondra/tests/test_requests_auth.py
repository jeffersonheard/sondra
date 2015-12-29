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
    u.save(conflict='replace')

    def teardown():
        u.delete()

    request.addfinalizer(teardown)
    return suite['auth']['users']['admin']


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
    suite['auth']['users'].create_user(username='app_level', password='password', email='app_level@sondra.github.com')
    u = suite['auth']['users']['app_level']

    r = suite['auth']['roles'].create({
        "title": "Application-Level Role",
        "description": "Application-Level Authority",
        "permissions": [
            {
                "application": "authorized-app",
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
                "application": "authorized-app",
                "collection": "authorized-documents",
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
        "username": u['username'], "password": "password"
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


@pytest.fixture
def admin_jwt(request, administrator):
    jwt = _login(administrator)
    
    def teardown():
        _logout(jwt)
        
    request.addfinalizer(teardown)
    
    
@pytest.fixture
def basic_jwt(request, basic_user):
    jwt = _login(basic_user)
    
    def teardown():
        _logout(jwt)
        
    request.addfinalizer(teardown)
    

@pytest.fixture
def collection_level_jwt(request, collection_level_user):
    jwt = _login(administrator)
    
    def teardown():
        _logout(jwt)
        
    request.addfinalizer(teardown)
    
    
@pytest.fixture
def app_level_jwt(request, application_level_user):
    jwt = _login(administrator)
    
    def teardown():
        _logout(jwt)
        
    request.addfinalizer(teardown)


@pytest.fixture
def docs():
    return [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]


def _call(rel, jwt=None, data=None):
    rel = 'http://localhost:5000/api/' + rel
    if jwt:
        if data:
            result = requests.post(rel, headers=_auth_header(jwt), data=json.dumps(data))
        else:
            result = requests.get(rel, headers=_auth_header(jwt))
    else:
        if data:
            result = requests.post(rel, data=json.dumps(data))
        else:
            result = requests.get(rel)

    return result


def _auth_header(jwt):
    return {"Authorization": "Bearer "+jwt}


def test_all_users(suite, basic_user, application_level_user, collection_level_user):
    assert len(suite['auth']['users']) == 3
    assert len(suite['auth']['user-credentials']) == 3
    assert len({cred['user'] for cred in suite['auth']['user-credentials']}) == 3
    assert len({cred['secret'] for cred in suite['auth']['user-credentials']}) == 3
    
    basic_jwt = _login(basic_user)
    application_level_jwt = _login(application_level_user)
    collection_level_jwt = _login(collection_level_user)

    assert len(suite['auth']['logged-in-users']) == 3

    _logout(basic_jwt)
    _logout(application_level_jwt)
    _logout(collection_level_jwt)


def test_login_logout(basic_user):
    jwt = _login(basic_user)

    result = requests.post('http://localhost:5000/api/simple-app.authenticated-method', headers=_auth_header(jwt))
    assert result.ok, result.text

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
    assert not result.ok

    # should be able to use the new token
    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents;json', headers=_auth_header(new_jwt))
    assert result.ok, result.text

    _logout(new_jwt)


def test_signup(suite):
    result = _call('auth/users.signup', data={
        'username': 'signup',
        'password': 'password',
        'email': 'signup@sondra.github.com',
        'given_name': "Jefferson",
        'family_name': "Heard"
    })
    assert result.ok, result.text

    assert 'signup' in suite['auth']['users']
    u = suite['auth']['users']['signup']
    assert u['active'] is False
    assert u['confirmed_email'] is False

    result = _call('auth/users.signup', data={
        'username': 'signup',
        'password': 'password',
        'email': 'signup@sondra.github.com',
        'given_name': "Jefferson",
        'family_name': "Heard"
    })
    assert not result.ok, "Duplicate signup should not be allowed"

    u.delete()


def test_anonymous_meta():
    result = requests.get('http://localhost:5000/api/simple-app;schema')
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/simple-app;help')
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app;schema')
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authenticated-app;help')
    assert result.ok, result.text


def test_admin_method_access(suite, administrator):
    jwt = _login(administrator)
    headers = _auth_header(jwt)

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authenticated-method;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authorized-method;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/simple-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    _logout(jwt)


def test_admin_inheritor_documents(suite, administrator, docs):
    jwt = _login(administrator)
    headers = _auth_header(jwt)
   
    #
    # Test adding and deleting a document as as admin
    #
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    ##
    #
    inheritor_documents = 'http://localhost:5000/api/authorized-app/simple-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # add an item to the collection
    post = requests.post(inheritor_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_empty.ok

    _logout(jwt)


def test_admin_authenticated_documents(suite, administrator, docs):
    jwt = _login(administrator)
    headers = _auth_header(jwt)

    ##
    #
    authenticated_documents = 'http://localhost:5000/api/authenticated-app/authenticated-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authenticated_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authenticated_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)


def test_admin_authorized_documents(suite, administrator, docs):
    jwt = _login(administrator)
    headers = _auth_header(jwt)

    ##
    #
    authorized_documents = 'http://localhost:5000/api/authorized-app/authorized-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authorized_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authorized_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)

#
# Basic User
#

def test_basic_method_access(suite, basic_user):
    jwt = _login(basic_user)
    headers = _auth_header(jwt)

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authenticated-method;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authorized-method;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/simple-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    _logout(jwt)


def test_basic_inheritor_documents(suite, basic_user, docs):
    jwt = _login(basic_user)
    headers = _auth_header(jwt)
   
    #
    # Test adding and deleting a document as as admin
    #
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    ##
    #
    inheritor_documents = 'http://localhost:5000/api/authorized-app/simple-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    # add an item to the collection
    post = requests.post(inheritor_documents, headers=headers, data=json.dumps(docs))
    assert not post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_empty.ok

    _logout(jwt)


def test_basic_authenticated_documents(suite, basic_user, docs):
    jwt = _login(basic_user)
    headers = _auth_header(jwt)

    ##
    #
    authenticated_documents = 'http://localhost:5000/api/authenticated-app/authenticated-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authenticated_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authenticated_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)


def test_basic_authorized_documents(suite, basic_user, docs):
    jwt = _login(basic_user)
    headers = _auth_header(jwt)

    ##
    #
    authorized_documents = 'http://localhost:5000/api/authorized-app/authorized-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authorized_documents, headers=headers, data=json.dumps(docs))
    assert not post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authorized_documents + ';json', headers=headers)
    assert not get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    _logout(jwt)

#
# application level User
#

def test_application_user_method_access(suite, application_level_user):
    jwt = _login(application_level_user)
    headers = _auth_header(jwt)

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authenticated-method;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authorized-method;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/simple-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    _logout(jwt)


def test_application_user_inheritor_documents(suite, application_level_user, docs):
    jwt = _login(application_level_user)
    headers = _auth_header(jwt)
   
    #
    # Test adding and deleting a document as as admin
    #
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    ##
    #
    inheritor_documents = 'http://localhost:5000/api/authorized-app/simple-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # add an item to the collection
    post = requests.post(inheritor_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_empty.ok

    _logout(jwt)


def test_application_user_authenticated_documents(suite, application_level_user, docs):
    jwt = _login(application_level_user)
    headers = _auth_header(jwt)

    ##
    #
    authenticated_documents = 'http://localhost:5000/api/authenticated-app/authenticated-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authenticated_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authenticated_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)


def test_application_user_authorized_documents(suite, application_level_user, docs):
    jwt = _login(application_level_user)
    headers = _auth_header(jwt)

    ##
    #
    authorized_documents = 'http://localhost:5000/api/authorized-app/authorized-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authorized_documents, headers=headers, data=json.dumps(docs))
    assert not post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authorized_documents + ';json', headers=headers)
    assert not get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert not confirmed_dangerous_delete.ok

    _logout(jwt)
#
# collection level User
#

def test_collection_user_method_access(suite, collection_level_user):
    jwt = _login(collection_level_user)
    headers = _auth_header(jwt)

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authenticated-method;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/simple-app/simple-documents.authorized-method;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/simple-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    result = requests.get('http://localhost:5000/api/authenticated-app/authenticated-documents.simple-none-return;json', headers=headers)
    assert result.ok, result.text

    result = requests.get('http://localhost:5000/api/authorized-app/authorized-documents.simple-none-return;json', headers=headers)
    assert not result.ok, result.text

    _logout(jwt)


def test_collection_user_inheritor_documents(suite, collection_level_user, docs):
    jwt = _login(collection_level_user)
    headers = _auth_header(jwt)
   
    #
    # Test adding and deleting a document as as admin
    #
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    ##
    #
    inheritor_documents = 'http://localhost:5000/api/authorized-app/simple-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # add an item to the collection
    post = requests.post(inheritor_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(inheritor_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(inheritor_documents + ';json', headers=headers)
    assert get_empty.ok

    _logout(jwt)


def test_collection_user_authenticated_documents(suite, collection_level_user, docs):
    jwt = _login(collection_level_user)
    headers = _auth_header(jwt)

    ##
    #
    authenticated_documents = 'http://localhost:5000/api/authenticated-app/authenticated-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authenticated_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authenticated_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authenticated_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)


def test_collection_user_authorized_documents(suite, collection_level_user, docs):
    jwt = _login(collection_level_user)
    headers = _auth_header(jwt)

    ##
    #
    authorized_documents = 'http://localhost:5000/api/authorized-app/authorized-documents'
    #

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': False})
    assert not dangerous_delete.ok

    # add an item to the collection
    post = requests.post(authorized_documents, headers=headers, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(authorized_documents + ';json', headers=headers)
    assert get_all.ok

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(authorized_documents, headers=headers, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    _logout(jwt)

