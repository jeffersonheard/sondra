import pytest

from sondra.suite import SuiteException
from .api import *
from sondra.application import Application


def _ignore_ex(f):
    try:
        f()
    except SuiteException:
        pass


@pytest.fixture()
def s(request):
    v = ConcreteSuite()
    _ignore_ex(lambda: EmptyApp(v))
    _ignore_ex(lambda: DerivedApp(v))
    _ignore_ex(lambda: SimpleApp(v))
    _ignore_ex(lambda: SimpleApp(v, "Alt"))
    return v


def test_suite_apps(s):
    """Make sure apps were registered in the suite"""
    assert 'empty-app' in s
    assert 'simple-app' in s
    assert 'alt' in s
    assert len(s) == 4, "Wrong number of apps was {0} should be 4: {1}".format(len(s.keys()), [x for x in s.keys()])
    assert all([isinstance(x, Application) for x in s.values()])

    assert s['simple-app'].db == 'simple_app'
    assert s['alt'].db == 'alt'


def test_suite_properties(s):
    """Make sure that properties are consistent after init"""
    assert s.slug == 'api'


def test_suite_schema(s):
    """Make sure the schema is structured as expected"""
    assert isinstance(s.schema, dict)
    assert s.schema['title'] == "Sondra-Based API"
    assert s.schema['id'] == "http://localhost:5000/api;schema"
    assert s.schema['description'] == (s.__doc__ or "*No description provided.*")
    assert all([s.schema['applications'][k] == v.url for k, v in s.items()])
    assert 'point' in s.schema['definitions']
    assert 'concreteSuiteDefn' in s.schema['definitions']


def test_help(s):
    """Make sure that the help method returns something, even in edge cases"""
    assert isinstance(s.help(), str)