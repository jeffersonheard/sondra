import pytest
import requests
import json

from .api import *

BASE_URL = ConcreteSuite.url

def _url(*args):
    return '/'.join((BASE_URL,) + args)

@pytest.fixture()
def file_document():
    return {
        "name": "file document 1"
    }

def test_add_delete_filedocument(file_document):
    destination = _url('simple-app', 'file-documents')

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(destination, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # add an item to the collection
    with open("sondra/tests/data/test.json", 'rb') as post_file:
        post = requests.post(destination, data={"__objs": json.dumps(file_document)}, files={"file": post_file})
        assert post.ok

    # get all the docs added (one)
    get_all = requests.get(destination + ';json')
    assert get_all.ok
    assert len(get_all.json()) == 1
    doc = get_all.json()[0]
    assert 'file' in doc
    assert doc['file'].startswith('http')

    get_file = requests.get(doc['file'])
    assert get_file.ok
    with open("sondra/tests/data/test.json") as input_file:
        assert json.load(input_file) == get_file.json()


    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(destination, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok
