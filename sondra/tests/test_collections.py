import pytest

from sondra.suite import SuiteException
from .api import *
from sondra.collection import Collection

def _ignore_ex(f):
    try:
        f()
    except SuiteException:
        pass


@pytest.fixture(scope='module')
def s(request):
    v = ConcreteSuite()
    _ignore_ex(lambda: EmptyApp(v))
    _ignore_ex(lambda: DerivedApp(v))
    _ignore_ex(lambda: SimpleApp(v))
    _ignore_ex(lambda: SimpleApp(v, "Alt"))
    v.ensure_database_objects()
    return v


def test_collection_methods_local(s):
    assert s['simple-app']['simple-documents'].simple_none_return() is None
    assert s['simple-app']['simple-documents'].simple_int_return() == 1
    assert s['simple-app']['simple-documents'].simple_number_return() == 1.0
    assert s['simple-app']['simple-documents'].simple_str_return() == "String"
    assert s['simple-app']['simple-documents'].list_return() == ["0", "1", "2", "3"]
    assert s['simple-app']['simple-documents'].dict_return() == {'a': 0, 'b': 1, 'c': 2}
    assert s['simple-app']['simple-documents'].operates_on_self() == s['simple-app']['simple-documents'].title


def test_derived_collection_inheritance(s):
    """Make sure that inheritance never modifies the base class and that appropriate attributes are merged"""
    base_coll = s['simple-app']['simple-documents']
    derived_coll = s['derived-app']['derived-collection']

    assert hasattr(derived_coll, 'simple_none_return')
    assert hasattr(derived_coll, 'derived_method')

    assert not hasattr(base_coll, 'derived_method')


def test_collection_construction(s):
    coll = s['simple-app']['simple-documents']


def test_collection_properties(s):
    coll = s['simple-app']['simple-documents']

    assert coll.suite
    assert coll.application
    assert coll.url == coll.application.url + '/' + coll.slug
    assert coll.table


def test_collection_schema(s):
    assert 'id' in s['simple-app']['simple-documents'].schema
    assert s['simple-app']['simple-documents'].schema['id'].startswith(s['simple-app']['simple-documents'].url)


def test_abstract_collection(s):
    class AbstractCollection(Collection):
        "An abstract collection"

        @expose_method
        def exposed_method(self) -> None:
            return None


    class ConcreteCollection(AbstractCollection):
        document_class = SimpleDocument


    assert AbstractCollection.abstract
    assert not ConcreteCollection.abstract


def test_collection_help(s):
    assert s['simple-app']['simple-documents'].help()
    assert s['simple-app']['simple-points'].help()
    assert s['simple-app']['foreign-key-docs'].help()