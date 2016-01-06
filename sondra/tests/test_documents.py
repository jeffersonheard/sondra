import pytest

from sondra.suite import SuiteException
from .api import *
from datetime import datetime


def _ignore_ex(f):
    try:
        f()
    except SuiteException:
        pass


@pytest.fixture(scope='module')
def s(request):
    v = ConcreteSuite()
    _ignore_ex(lambda: SimpleApp(v))
    _ignore_ex(lambda: DerivedApp(v))
    v.clear_databases()
    return v


@pytest.fixture()
def simple_document(request, s):
    doc = s['simple-app']['simple-documents'].create({
        'name': "Document 1",
        'date': datetime.now(),
    })
    def teardown():
        doc.delete()
    request.addfinalizer(teardown)

    return doc


@pytest.fixture()
def file_document(request, s):
    doc = s['simple-app']['file-documents'].create({
        "name": "file document 1",
        "file": open("sondra/tests/data/test.json")
    })
    def teardown():
        doc.delete()
    request.addfinalizer(teardown)

    return doc

@pytest.fixture()
def foreign_key_document(request, s, simple_document):
    doc = s['simple-app']['foreign-key-docs'].create({
        'name': 'FK Doc',
        'simple_document': simple_document,
        'rest': [simple_document, simple_document, simple_document]
    })
    def teardown():
        doc.delete()
    request.addfinalizer(teardown)

    return doc


@pytest.fixture()
def simple_point(request, s):
    doc = s['simple-app']['simple-points'].create({
        'name': "A Point",
        'date': datetime.now(),
        'geometry': {"type": "Point", "coordinates": [-85.1, 31.8]}
    })
    def teardown():
        doc.delete()
    request.addfinalizer(teardown)

    return doc


def test_document_methods_local(s):
    assert s['simple-app']['simple-documents'].simple_none_return() is None
    assert s['simple-app']['simple-documents'].simple_int_return() == 1
    assert s['simple-app']['simple-documents'].simple_number_return() == 1.0
    assert s['simple-app']['simple-documents'].simple_str_return() == "String"
    assert s['simple-app']['simple-documents'].list_return() == ["0", "1", "2", "3"]
    assert s['simple-app']['simple-documents'].dict_return() == {'a': 0, 'b': 1, 'c': 2}
    assert s['simple-app']['simple-documents'].operates_on_self() == s['simple-app']['simple-documents'].title


def test_file_storage(s, file_document):
    result = s.file_storage._table(file_document.collection).get_all(file_document.id, "document").run(s.file_storage._conn(file_document.collection))
    file_record = next(result)
    assert file_record['original_file'] == 'test.json'
    assert file_record['content_type'] == 'application/octet-stream'

    with open('sondra/tests/data/test.json') as infile:
        original_data = infile.read()

    with file_document['file'] as infile:
        test_data = infile.read()

    assert original_data == test_data


def test_derived_document_inheritance(s):
    """Make sure that inheritance never modifies the base class and that appropriate attributes are merged"""
    base_coll = s['simple-app']['simple-documents']
    derived_coll = s['derived-app']['derived-collection']

    assert hasattr(derived_coll, 'simple_none_return')
    assert hasattr(derived_coll, 'derived_method')

    assert not hasattr(base_coll, 'derived_method')


def test_document_construction(s):
    coll = s['simple-app']['simple-documents']


def test_document_properties(s):
    coll = s['simple-app']['simple-documents']

    assert coll.suite
    assert coll.application
    assert coll.url == coll.application.url + '/' + coll.slug
    assert coll.table


def test_simple_document_creation(s, simple_document):
    assert simple_document.id
    assert simple_document.id == simple_document.slug
    assert simple_document.slug is not None
    assert simple_document.slug == 'document-1'
    assert 'document-1' in simple_document.collection
    assert simple_document['date'] is not None
    assert simple_document['timestamp'] is not None
    assert simple_document['value'] == 0  # make sure defaults work


def test_document_update(s, simple_document):
    simple_document['value'] = 1024
    simple_document.save(conflict='replace')
    updated = s['simple-app']['simple-documents'][simple_document.id]
    assert updated['value'] == 1024


def test_foreign_key_doc_creation(s, foreign_key_document):
    single = foreign_key_document.fetch('simple_document')
    multiple = foreign_key_document.fetch('rest')

    assert isinstance(single, SimpleDocument)
    assert isinstance(multiple, list)
    assert all([isinstance(x, SimpleDocument) for x in multiple])
    assert isinstance(foreign_key_document['simple_document'], str)
    assert isinstance(foreign_key_document['rest'], list)
    assert all([isinstance(x, str) for x in foreign_key_document['rest']])


def test_simple_point_creation(s, simple_point):
    assert simple_point['geometry']

def test_document_help(s):
    assert s['simple-app']['simple-documents'].help()
    assert s['simple-app']['simple-points'].help()
    assert s['simple-app']['foreign-key-docs'].help()