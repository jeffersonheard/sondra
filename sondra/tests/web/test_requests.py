import json
import requests

from . import documents

BASE_URL = 'http://localhost:5000'
def _url(*args):
    return '/'.join((BASE_URL,) + args)

def test_suite_schema():
    """Get the schema for the whole suite of APIs"""
    r = requests.get(_url('api/schema'))
    assert r.ok
    assert r.json()


def test_app_schema():
    """Get the schema for a single app"""
    r = requests.get(_url('api/base-app;schema'))
    assert r.ok
    assert r.json()


def test_collection_schema():
    """Get the schema for a single collection"""
    r = requests.get(_url('api/base-app/tracked-item-templates;schema'))
    assert r.ok
    assert r.json()


def test_app_help():
    """Get the help text for the whole application"""
    r = requests.get(_url('api/base-app;help'))
    assert r.ok
    assert r.text


def test_app_method():
    """Test all aspects of an app method"""
    test_method_url = _url('api/base-app.test-app-method')

    schema_url = test_method_url + ';schema'
    schema = requests.get(schema_url)
    assert schema.ok
    schema = schema.json()
    assert isinstance(schema, dict)
    assert 'definitions' in schema
    assert 'request' in schema['definitions']
    assert 'response' in schema['definitions']
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


def test_collection_method():
    test_method_url = _url('api/base-app/tracked-item-templates.test-collection-method')

    schema_url = test_method_url + ';schema'
    schema = requests.get(schema_url)
    assert schema.ok
    schema = schema.json()
    assert isinstance(schema, dict)
    assert 'definitions' in schema
    assert 'request' in schema['definitions']
    assert 'response' in schema['definitions']
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


def test_document_method():
    pass


def test_add_delete_document():
    tracked_item_templates = _url('api/base-app/tracked-item-templates')
    tracked_item_template1 = {
        "name": "test_template",
        "category": "barcoded",
        "baseGeoemtry": {"type": "Point", "coordinates": [1.5, 1.5]},
        "properties": {
            "prop1": True,
            "prop2": 0,
            "prop3": "yes"
        }
    }

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(tracked_item_templates, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    confirmed_dangerous_delete = requests.delete(tracked_item_templates, params={'delete_all': False})
    assert not confirmed_dangerous_delete.ok

    # try to delete all documents in a collection, but don't specify "delete_all" to confirm
    get_empty = requests.get(tracked_item_templates + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0

    # add an item to the collection
    post = requests.post(tracked_item_templates, data=json.dumps(tracked_item_template1))
    assert post.ok

    # attempt to delete everything, but fail
    dangerous_delete = requests.delete(tracked_item_templates)
    assert not dangerous_delete.ok

    # get the item we just added
    get_one = requests.get(tracked_item_templates + '/test_template' + ';json')
    assert get_one.ok
    assert get_one.json() == tracked_item_template1

    # get the help for a document
    get_one = requests.get(tracked_item_templates + '/test_template' + ';help')
    assert get_one.ok
    assert get_one.text

    # get the schema for a document
    get_one = requests.get(tracked_item_templates + '/test_template' + ';schema')
    assert get_one.ok
    assert get_one.json() == documents.TrackedItemTemplates.schema

    # get all the docs added (one)
    get_all = requests.get(tracked_item_templates + ';json')
    assert get_all.ok
    assert len(get_all.json()) == 1

    # delete the doc that we added
    delete = requests.delete(tracked_item_templates + '/test_template')
    assert delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(tracked_item_templates + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0


def test_add_delete_documents():
    tracked_item_templates = _url('api/base-app/tracked-item-templates')
    tracked_items = _url('api/base-app/tracked-items')
    tracked_item_template1 = {
        "name": "test_template",
        "category": "barcoded",
        "baseGeoemtry": {"type": "Point", "coordinates": [1.5, 1.5]},
        "properties": {
            "prop1": True,
            "prop2": 0,
            "prop3": "yes"
        }
    }

    # delete all documents in a collection
    confirmed_dangerous_delete = requests.delete(tracked_item_templates, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok
    confirmed_dangerous_delete = requests.delete(tracked_items, params={'delete_all': True})
    assert confirmed_dangerous_delete.ok

    # delete all documents in a collection, set delete_all to false and throw an error
    dangerous_delete = requests.delete(tracked_item_templates, params={'delete_all': False})
    assert not dangerous_delete.ok

    # try to delete all documents in a collection, but don't specify "delete_all" to confirm
    get_empty = requests.get(tracked_item_templates + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0

    # add an item to the collection
    post = requests.post(tracked_item_templates, data=json.dumps(tracked_item_template1))
    assert post.ok

    tracked_item_template1_ref = post.json()[0]
    tracked_items_data = [{
        'barcode': 'ABCD' + str(x).zfill(3),
        'template': tracked_item_template1_ref,
        'location': {"type": "Point", "coordinates": [1.5, 1.5]}
    } for x in range(100)]
    tracked_items_post = requests.post(tracked_items, data=json.dumps(tracked_items_data))
    assert tracked_items_post.ok

    tracked_items_keys = tracked_items_post.json()
    assert len(tracked_items_keys) == len(tracked_items_data)
    barcodes = sorted([b['barcode'] for b in tracked_items_data])
    for i, x in enumerate(sorted(tracked_items_keys)):
        assert x.endswith(barcodes[i])

    # attempt to delete everything, but fail
    dangerous_delete = requests.delete(tracked_item_templates)
    assert not dangerous_delete.ok

    # get the item we just added
    get_one = requests.get(tracked_item_templates + '/test_template' + ';json')
    assert get_one.ok
    assert get_one.json() == tracked_item_template1

    # get the help for a document
    get_one = requests.get(tracked_item_templates + '/test_template' + ';help')
    assert get_one.ok
    assert get_one.text

    # get the schema for a document
    get_one = requests.get(tracked_item_templates + '/test_template' + ';schema')
    assert get_one.ok
    assert get_one.json() == documents.TrackedItemTemplates.schema

    # get the first page of the docs added (one)
    get_all = requests.get(tracked_item_templates + ';json')
    assert get_all.ok
    assert len(get_all.json()) < len(barcodes)

    # delete the doc that we added
    delete = requests.delete(tracked_item_templates + '/test_template')
    assert delete.ok

    # ensure there are no docs left in the data store
    get_empty = requests.get(tracked_item_templates + ';json')
    assert get_empty.ok
    assert len(get_empty.json()) == 0


def test_update_document():
    pass


def test_get_document():
    pass


def test_get_documents():
    pass


def test_geojson_document():
    pass


def test_geojson_documents():
    pass


def test_filters():
    pass

