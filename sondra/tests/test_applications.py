import pytest
from .api import *
from sondra.collection import Collection


@pytest.fixture(scope='module')
def s(request):
    v = ConcreteSuite()
    EmptyApp(v)
    DerivedApp(v)
    SimpleApp(v)
    SimpleApp(v, "Alt")
    return v


def test_app_collections(s):
    "Make sure all collections are in the app, and that all contents of app are collections"
    assert 'simple-documents' in s['simple-app']
    assert 'simple-points' in s['simple-app']
    assert 'foreign-key-docs' in s['simple-app']
    assert len(s['simple-app']) == 4
    assert all([isinstance(x, Collection) for x in s['simple-app'].values()])

    assert 'simple-documents' not in s['empty-app']


def test_application_methods_local(s):
    """Make sure that all local application methods still act like methods"""
    assert s['simple-app'].simple_none_return() is None
    assert s['simple-app'].simple_int_return() == 1
    assert s['simple-app'].simple_number_return() == 1.0
    assert s['simple-app'].simple_str_return() == "String"
    assert s['simple-app'].list_return() == ["0", "1", "2", "3"]
    assert s['simple-app'].dict_return() == {'a': 0, 'b': 1, 'c': 2}
    assert s['simple-app'].operates_on_self() == s['simple-app'].title


def test_derived_app_inheritance(s):
    """Make sure that inheritance never modifies the base class and that appropriate attributes are merged"""
    assert hasattr(s['derived-app'], 'simple_none_return')
    assert hasattr(s['derived-app'], 'derived_method')
    assert 'derived-collection' in s['derived-app']

    assert not hasattr(s['simple-app'], 'derived_method')
    assert 'derived-collection' not in s['simple-app']


def test_app_construction(s):
    """Make sure that apps are consistent post-construction"""
    empty_app = s['empty-app']
    simple_app = s['simple-app']
    derived_app = s['derived-app']

    assert hasattr(empty_app, "definitions")
    assert empty_app.definitions == {}
    assert 'appDef' in simple_app.definitions
    assert 'appDef' in derived_app.definitions
    assert 'derivedDef' in derived_app.definitions
    assert 'derivedDef' not in simple_app.definitions


def test_app_properties(s):
    """Make sure that calculated properties of the app are correct"""
    simple_app = s['simple-app']
    derived_app = s['derived-app']

    assert simple_app.url == (s.url + '/simple-app')
    assert simple_app.title == "Simple App"
    assert derived_app.url == (s.url + '/derived-app')
    assert derived_app.title == "Derived App"


def test_app_schema(s):
    """Make sure the schema contains everything it should"""
    simple_app = s['simple-app']
    simple_app_schema = simple_app.schema

    assert 'definitions' in simple_app_schema
    assert 'appDef' in simple_app_schema['definitions']

    assert simple_app_schema['type'] == 'object'
    assert 'methods' in simple_app_schema
    assert 'simple-none-return' in simple_app_schema['methods']
    assert 'derived-method' not in simple_app_schema['methods']
    assert simple_app_schema['id'] == (s.url + '/' + simple_app.slug + ';schema')
    assert simple_app_schema['type'] == 'object'
    assert all([x in simple_app_schema['collections'] for x in simple_app])
    assert all([simple_app_schema['collections'][k] == v.url for k, v in simple_app.items()])
    assert simple_app_schema['description'] == (simple_app.__doc__ or "*No description provided*")

def test_help(s):
    """Make sure help returns something, even in edge cases"""
    assert isinstance(s['simple-app'].help(), str)
    assert isinstance(s['derived-app'].help(), str)
    assert isinstance(s['empty-app'].help(), str)