import pytest
from datetime import datetime
import json
import requests

from sondra.tests import api

BASE_URL = api.ConcreteSuite.url

@pytest.fixture
def docs(request):
    simple_documents = _url('simple-app/simple-documents')
    docs = [{
        "name": "Added Document {0}".format(x),
        "value": x,
        "date": datetime.utcnow().isoformat()
    } for x in range(10)]

    assert requests.delete(simple_documents, params={'delete_all': True}).ok
    assert requests.post(simple_documents, data=json.dumps(docs)).ok
    docs = requests.get(simple_documents)

    def teardown():
        assert requests.delete(simple_documents, params={'delete_all': True}).ok

    request.addfinalizer(teardown)
    return docs


@pytest.fixture
def points(request):
    simple_points = _url('simple-app/simple-points')
    points = [{
        "name": "Added Point {0}".format(x),
        "date": datetime.utcnow().isoformat(),
        "geometry": {"type": "Point", "coordinates": [-10.1*x, 33.2]}
    } for x in range(10)]

    assert requests.delete(simple_points, params={'delete_all': True}).ok
    assert requests.post(simple_points, data=json.dumps(points)).ok
    points = requests.get(simple_points)

    def teardown():
        assert requests.delete(simple_points, params={'delete_all': True}).ok

    request.addfinalizer(teardown)
    return points



def _url(*args):
    return '/'.join((BASE_URL,) + args)


def test_suite_schema():
    """Get the schema for the whole suite of APIs"""
    r = requests.get(BASE_URL + ';schema')
    assert r.ok
    assert r.json()


def test_app_schema():
    """Get the schema for a single app"""
    r = requests.get(_url('simple-app;schema'))
    assert r.ok
    assert r.json()


def test_collection_schema():
    """Get the schema for a single collection"""
    r = requests.get(_url('simple-app/simple-documents;schema'))
    assert r.ok
    assert r.json()


def test_app_help():
    """Get the help text for the whole application"""
    r = requests.get(_url('simple-app;help'))
    assert r.ok
    assert r.text


def test_method_returns():
    int_method_url = _url('simple-app/simple-documents.simple-int-return')
    none_method_url = _url('simple-app/simple-documents.simple-none-return')
    number_method_url = _url('simple-app/simple-documents.simple-number-return')
    str_method_url = _url('simple-app/simple-documents.simple-str-return')
    list_method_url = _url('simple-app/simple-documents.list-return')
    dict_method_url = _url('simple-app/simple-documents.dict-return')
    self_method_url = _url('simple-app/simple-documents.operates-on-self')
    
    int_rsp = requests.post(int_method_url)
    assert int_rsp.ok
    x = int_rsp.json()
    assert isinstance(x, dict)
    assert '_' in x
    assert isinstance(x['_'], int)
    
    none_rsp = requests.post(none_method_url)
    assert none_rsp.ok
    
    number_rsp = requests.post(number_method_url)
    assert number_rsp.ok
    x = number_rsp.json()
    assert isinstance(x, dict)
    assert '_' in x
    assert isinstance(x['_'], float)
    
    str_rsp = requests.post(str_method_url)
    assert str_rsp.ok
    x = str_rsp.json()
    assert isinstance(x, dict)
    assert '_' in x
    assert isinstance(x['_'], str)
    
    dict_rsp = requests.post(dict_method_url)
    assert dict_rsp.ok
    x = dict_rsp.json()
    assert isinstance(x, dict)
    assert '_' not in x
    assert x == {
        'a': 0,
        'b': 1,
        'c': 2
    }

    list_rsp = requests.post(list_method_url)
    assert list_rsp.ok
    x = list_rsp.json()
    assert isinstance(x, list)
    assert x == ["0", "1", "2", "3"]
    
    self_rsp = requests.post(self_method_url)
    assert self_rsp.ok


def test_app_method():
    """Test all aspects of an app method"""
    test_method_url = _url('simple-app.arg-test')

    schema_url = test_method_url + ';schema'
    schema = requests.get(schema_url)
    assert schema.ok
    schema = schema.json()
    assert isinstance(schema, dict)
    assert 'definitions' in schema
    assert 'method_request' in schema['definitions']
    assert 'method_response' in schema['definitions']
    assert 'id' in schema
    assert schema['id'] == schema_url

    help_url = test_method_url + ';help'
    help = requests.get(help_url)
    assert help.ok
    assert help.text

    args = json.dumps({
        'int_arg': 10,
        'str_arg': "string",
        'list_arg': ['list'],
        'dict_arg': {'key': 'value'}
    })

    json_url = test_method_url + ';json'
    get_noargs = requests.get(json_url)
    get_args = requests.get(json_url, params={'q': args})

    post_noargs = requests.post(json_url)
    post_args = requests.post(json_url, data=args)

    assert get_noargs.ok
    assert get_noargs.json()
    assert get_args.ok
    assert get_args.json()
    assert post_args.ok
    assert post_args.json()
    assert get_args.json() == post_args.json()
    assert get_noargs.json() == post_noargs.json()
    assert post_args.json() == [{"one": 1}, 0, 2]


def test_collection_method_schema():
    test_method_url = _url('simple-app/simple-documents.simple-int-return')

    schema_url = test_method_url + ';schema'
    schema = requests.get(schema_url)
    assert schema.ok
    schema = schema.json()
    assert isinstance(schema, dict)
    assert 'definitions' in schema
    assert 'method_request' in schema['definitions']
    assert 'method_response' in schema['definitions']
    assert 'id' in schema
    assert schema['id'] == schema_url


def test_collection_method_help():
    test_method_url = _url('simple-app/simple-documents.simple-int-return')

    help_url = test_method_url + ';help'
    help = requests.get(help_url)
    assert help.ok
    assert help.text


def test_document_method():
    simple_documents = _url('simple-app/simple-documents')
    added_document_1 = {
        "name": "Added Document 1",
        "date": datetime.utcnow().isoformat(),
    }
    # add an item to the collection
    post = requests.post(simple_documents, data=json.dumps(added_document_1))
    assert post.ok

    test_method_url = _url('simple-app/simple-documents/added-document-1.arg-test')

    schema_url = test_method_url + ';schema'
    schema = requests.get(schema_url)
    assert schema.ok
    schema = schema.json()
    assert isinstance(schema, dict)
    assert 'definitions' in schema
    assert 'method_request' in schema['definitions']
    assert 'method_response' in schema['definitions']
    assert 'id' in schema
    assert schema['id'] == schema_url

    help_url = test_method_url + ';help'
    help = requests.get(help_url)
    assert help.ok
    assert help.text

    args = json.dumps({
        'int_arg': 10,
        'str_arg': "string",
        'list_arg': ['list'],
        'dict_arg': {'key': 'value'}
    })

    json_url = test_method_url + ';json'
    get_noargs = requests.get(json_url)
    get_args = requests.get(json_url, params={'q': args})

    post_noargs = requests.post(json_url)
    post_args = requests.post(json_url, data=args)

    assert get_noargs.ok
    assert get_noargs.json()
    assert get_args.ok
    assert get_args.json()
    assert post_args.ok
    assert post_args.json()
    assert get_args.json() == post_args.json()
    assert get_noargs.json() == post_noargs.json()
    assert post_args.json() == [{"one": 1}, 0, 2]


def test_add_delete_document():
    simple_documents = _url('simple-app/simple-documents')
    added_document_1 = {
        "name": "Added Document 1",
        "date": datetime.utcnow().isoformat(),
    }
    added_document_2 = {
        "name": "Added Document 2",
        "date": datetime.utcnow().isoformat(),
    }

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': False})
    assert not confirmed_dangerous_delete.ok

    # try to delete all documents in a collection, but don't specify "delete_all" to confirm
    get_empty = requests.get(simple_documents + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0

    # add an item to the collection
    post = requests.post(simple_documents, data=json.dumps(added_document_1))
    assert post.ok

    # get all the docs added (one)
    get_all = requests.get(simple_documents + ';json')
    assert get_all.ok
    assert len(get_all.json()) == 1

    # attempt to delete everything, but fail
    dangerous_delete = requests.delete(simple_documents)
    assert not dangerous_delete.ok

    # get all the docs added (one)
    get_all = requests.get(simple_documents + ';json')
    assert get_all.ok
    assert len(get_all.json()) == 1

    # add an item to the collection
    post = requests.post(simple_documents, data=json.dumps(added_document_2))
    assert post.ok

    # get all the docs added (one)
    get_all = requests.get(simple_documents + ';json')
    assert get_all.ok
    assert len(get_all.json()) == 2

    # get the item we just added
    get_one = requests.get(simple_documents + '/added-document-1' + ';json')
    assert get_one.ok
    assert get_one.json()['name'] == added_document_1['name']

    # get the help for a document
    get_one = requests.get(simple_documents + '/added-document-1' + ';help')
    assert get_one.ok
    assert get_one.text

    # get the schema for a document
    get_one = requests.get(simple_documents + '/added-document-1' + ';schema')
    assert get_one.ok
    assert 'slug' in get_one.json()['properties']

    # delete the doc that we added
    delete = requests.delete(simple_documents + '/added-document-1')
    assert delete.ok

    # delete the doc that we added
    delete = requests.delete(simple_documents + '/added-document-2')
    assert delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(simple_documents + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0


def test_add_delete_documents():
    simple_documents = _url('simple-app/simple-documents')
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(simple_documents, params={'delete_all': False})
    assert not dangerous_delete.ok

    # try to delete all documents in a collection, but don't specify "delete_all" to confirm
    get_empty = requests.get(simple_documents + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0

    # add an item to the collection
    post = requests.post(simple_documents, data=json.dumps(docs))
    assert post.ok

    # get the first page of the docs added (one)
    get_all = requests.get(simple_documents + ';json?limit=100')
    assert get_all.ok
    assert len(get_all.json()) < len(docs)

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(simple_documents + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0


def test_update_document():
    simple_documents = _url('simple-app/simple-documents')
    document_1 = _url('simple-app/simple-documents/added-document-1')
    docs = [
        { "name": "Added Document {0}".format(x), "date": datetime.utcnow().isoformat() } for x in range(101)
    ]

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # add an item to the collection
    post = requests.post(simple_documents, data=json.dumps(docs))
    assert post.ok

    old_date = docs[0]['date']
    docs[0]['date'] = datetime.utcnow().isoformat()

    post = requests.post(document_1, data=json.dumps(docs[0]))
    assert post.ok

    get = requests.get(document_1)
    assert get.ok
    document_1 = get.json()
    assert document_1['date'] != old_date

    # delete the doc that we added
    confirmed_dangerous_delete = requests.delete(simple_documents, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok


def test_geojson_document(points):
    simple_points = _url('simple-app/simple-points')
    point_1 = _url('simple-app/simple-points/added-point-0;geojson')

    get = requests.get(point_1)
    assert get.ok
    pt_1 = get.json()
    assert 'type' in pt_1
    assert pt_1['type'] == 'Feature'
    assert 'date' in pt_1['properties']
    assert 'name' in pt_1['properties']

    get = requests.get(simple_points + ';geojson')
    assert get.ok
    assert 'type' in get.json()
    assert get.json()['type'] == 'FeatureCollection'
    assert pt_1['properties']['slug'] in [x['properties']['slug'] for x in get.json()['features']]


def test_geo__get_intersecting(points):
    simple_points = _url('simple-app/simple-points')

    bbox = requests.get(simple_points, params={
        "geo": json.dumps({
            "op": "get_intersecting",
            "test": {
                "type": "Polygon",
                "coordinates": [
                    [[-10.15, 33.0], [1.0, 33.0], [1.0, 33.5], [-10.15, 33.5], [-10.15, 33.0]]
                ]
            }
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 2  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)

    # Make sure that POSTs work the same as gets
    bbox_post = requests.post(simple_points, data=json.dumps({
        '__method': "GET",
        '__q': {
            "geo": {
                "op": "get_intersecting",
                "test": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-10.15, 33.0], [1.0, 33.0], [1.0, 33.5], [-10.15, 33.5], [-10.15, 33.0]]
                    ]
                }
            }
        }
    }))
    assert bbox_post.ok
    results = bbox_post.json()
    assert len(results) == 2


def test_geo__get_nearest(points):
    simple_points = _url('simple-app/simple-points')

    bbox = requests.get(simple_points, params={
        "geo": json.dumps({
            "op": "get_nearest",
            "test": {
                "type": "Point",
                "coordinates": [
                    -10.1, 33.2
                ]
            },
            "kwargs": {
                "max_dist": 100,
                "unit": "m",
            }
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 1  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)

    bbox_post = requests.post(simple_points, data=json.dumps({
        "__method": "GET",
        "__q": {
            "geo": {
                "op": "get_nearest",
                "test": {
                    "type": "Point",
                    "coordinates": [
                        -10.1, 33.2
                    ]
                },
                "kwargs": {
                    "max_dist": 100,
                    "unit": "m",
                }
            }
        }
    }))

    assert bbox_post.ok
    results = bbox_post.json()
    assert len(results) == 1


def test_flt__equals(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": "==",
            "lhs": "value",
            "rhs": 0
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 1  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__gt(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": ">",
            "lhs": "value",
            "rhs": 0
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 9  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__gte(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": ">=",
            "lhs": "value",
            "rhs": 0
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 10  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__lt(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": "<",
            "lhs": "value",
            "rhs": 5
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 5  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__lte(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": "<=",
            "lhs": "value",
            "rhs": 5
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 6  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__match(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": "match",
            "lhs": "name",
            "rhs": "Added Document [0-5]"
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 6  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


# def test_flt__contains(docs):
#     simple_documents = _url('simple-app/simple-documents')
#
#     bbox = requests.get(simple_documents, params={
#         "flt": json.dumps({
#             "op": "<=",
#             "lhs": "value",
#             "rhs": 5
#         })
#     })
#
#     assert bbox.ok
#     results = bbox.json()
#     assert len(results) == 6  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_flt__has_fields(docs):
    simple_documents = _url('simple-app/simple-documents')

    bbox = requests.get(simple_documents, params={
        "flt": json.dumps({
            "op": "has_fields",
            "fields": ["name","slug"]
        })
    })

    assert bbox.ok
    results = bbox.json()
    assert len(results) == 10  # should pick up the point at (0.0, 33.2) and (-10.1, 33.2)


def test_files():
    pass