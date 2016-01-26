from sondra.document.valuehandlers import DateTime, Geometry, Now
from shapely.geometry import Point
from datetime import datetime
import rethinkdb as r
import pytest

from sondra.tests.api import *
from sondra.auth import Auth

s = ConcreteSuite()

api = SimpleApp(s)
auth = Auth(s)
AuthenticatedApp(s)
AuthorizedApp(s)
s.ensure_database_objects()


@pytest.fixture(scope='module')
def simple_doc(request):
    simple_doc = s['simple-app']['simple-documents'].create({
        'name': "valuehandler test",
        "date": datetime.now(),
        "value": 0
    })
    def teardown():
        simple_doc.delete()
    request.addfinalizer(teardown)
    return simple_doc


@pytest.fixture(scope='module')
def fk_doc(request, simple_doc):
    fk_doc = s['simple-app']['foreign-key-docs'].create({
        'name': "valuehandler test foreign key",
        'simple_document': simple_doc,
        'rest': [simple_doc]
    })
    def teardown():
        fk_doc.delete()
    request.addfinalizer(teardown)
    return fk_doc


def test_foreignkey(fk_doc, simple_doc):
    retr_doc = s['simple-app']['foreign-key-docs']['valuehandler-test-foreign-key']

    # make sure our object representation is the JSON one in the retrieved object.
    assert isinstance(fk_doc.obj['simple_document'], str)
    assert fk_doc.obj['simple_document'] == simple_doc.url

    # make sure our object representation is the JSON one in the retrieved object.
    assert isinstance(retr_doc.obj['simple_document'], str)
    assert retr_doc.obj['simple_document'] == simple_doc.url

    storage_repr = fk_doc.rql_repr()
    assert storage_repr['simple_document'] == simple_doc.id

    assert isinstance(fk_doc['simple_document'], SimpleDocument)