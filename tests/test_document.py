import pytest

from sondra import document
from shapely.geometry import mapping, shape


# WHy do we do this?  We need to test that the environment created by the Environment constructor is
# ConcreteSuite() and not any of the base classes.  This tests the EnvironmentMetaclass
class BaseSuite(document.Suite):
    pass

class BaseSuite2(document.Suite):
    pass

class ConcreteSuite(BaseSuite, BaseSuite2):
    pass


class TrackedItemTemplate(document.Document):
    schema = {
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": {"type": "string", "description": "The template name. Must be unique in the collection"},
            "category": {"type": "string", "description": "The category this template belongs to, such as 'module'"},
            "baseGeometry": {"type": "object", "description": "GeoJSON geometry object"},
            "properties": {"type": "object", "description": "extra properties to set on the module, like manufacturer"},
        }
    }

class OtherApplication(document.Application):
    pass

class TerraHubBase(document.Application):
    pass

class TrackedItemTemplates(document.Collection):
    document_class = TrackedItemTemplate
    application = TerraHubBase
    primary_key = 'name'
    specials = {
        'baseGeometry': document.Geometry()
    }

class TrackedItem(document.Document):
    schema = {
        "type": "object",
        "properties": {
            "barcode": {"type": "string"},
            "template": {"type": "string"},
            "location": {"type": "object"},
            "properties": {"type": "object"}
        }
    }


class TrackedItems(document.Collection):
    document_class = TrackedItem
    application = TerraHubBase
    primary_key = "barcode"
    specials = {
        "location": document.Geometry("point")
    }
    references = [("template", TrackedItemTemplates)]
    indexes = ("template","location")


class TrackedItemHistory(document.Document):
    schema = {
        "type": "object",
        "properties": {
            "item": {"type": "string"},
            "location": {"type": "object"},
            "timestamp": {"type": "object"},
            "properties": {"type": "object"}
        }
    }

    
class TrackedItemHistories(document.Collection):
    document_class = TrackedItemHistory
    application = TerraHubBase
    specials = {
        "location": document.Geometry("point")
    }
    references = [("item", TrackedItem)]
    indexes = ("item", "location", "timestamp")


class ItemGroup(document.Document):
    schema = {
        "type": "object",
        "required": ["name","items"],
        "properties": {
            "parent": {"type": "string"},
            "name": {"type": "string"},
            "geometry": {"type": "object"},
            "items": {"type": "array", "items": {"type": "string"}}
        }
    }


class ItemGroups(document.Collection):
    document_class = ItemGroup
    application = TerraHubBase
    specials = {
        "geometry": document.Geometry('polygon')
    }
    references = [("parent", "self"), {"items": TrackedItems}]


@pytest.fixture(scope='module')
def apps(request):
    customer_jeff = TerraHubBase('jeff')
    customer_steve = TerraHubBase('steve')

    try:
        customer_jeff.create_database()
        customer_steve.create_database()
    except:
        pass

    try:
        customer_jeff.create_tables()
        customer_steve.create_tables()
    except:
        pass

    def fin():
        try:
            customer_jeff.drop_tables()
            customer_steve.drop_tables()
        except:
            pass

        try:
            customer_jeff.drop_database()
            customer_steve.drop_database()
        except:
            pass
    request.addfinalizer(fin)
    return (customer_jeff, customer_steve)


@pytest.fixture
def tracked_item_templates(request, apps):
    customer_jeff, customer_steve = apps
    chint300 = customer_jeff["tracked-item-templates"].create({
        "name": "CHINT-300.2014",
        "category": "module",
        "baseGeometry": {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.51], [0.0, 1.51], [0.0, 0.0]]]
        },
        "properties": {
            "manufacturer": "CHINT",
            "wattage": 300,
        }
    })
    ae_inverter = customer_steve["tracked-item-templates"].create({
        "name": "AE2310",
        "category": "inverter",
        "baseGeometry": {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]
        },
        "properties": {
            "manufacturer": "Advanced Energy",
            "wattage": 15000,
            "strings": 6
        }
    })
    def fin():
        ae_inverter.delete()
        chint300.delete()
    request.addfinalizer(fin)
    return chint300, ae_inverter


@pytest.fixture
def tracked_items(request, apps, tracked_item_templates):
    customer_jeff, customer_steve = apps
    chint300, ae_inverter = tracked_item_templates

    chint300_0001 = customer_jeff["tracked-items"].create({
        "barcode": "0001",
        "template": chint300,
        "properties": {
            "batch": "2014.00",
        }
    })
    chint300_0002 = customer_jeff["tracked-items"].create({
        "barcode": "0002",
        "template": chint300,
        "properties": {
            "batch": "2014.00",
        }
    })
    ae_inverter_0001 = customer_steve["tracked-items"].create({
        "barcode": "0001",
        "template": ae_inverter,
        "properties": {
            "manufacturerTested": True,
            "thirdPartyTested": True,
            "thirdPartyTester": "CEA"
        }
    })
    def fin():
        ae_inverter_0001.delete()
        chint300_0001.delete()
        chint300_0002.delete()
    request.addfinalizer(fin)
    return ae_inverter_0001, chint300_0001, chint300_0002



def test_application(apps):
    customer_jeff, customer_steve = apps
    app2 = OtherApplication()
    suite = document.Suite()

    # make sure the environment contains all the applicatoins
    assert suite.name == 'concrete_suite'
    assert 'jeff' in suite
    assert 'steve' in suite
    assert 'other-application' in suite

    # make sure that the databases are separate for the different application instances
    assert customer_jeff.db == 'jeff'
    assert customer_steve.db == 'steve'

    # make sure the application contains all the collections
    assert 'tracked-items' in customer_jeff
    assert 'tracked-item-templates' in customer_jeff
    assert 'tracked-item-histories' in customer_jeff

    # make sure that the application's collection registry doesn't span application classes
    assert 'tracked-items' not in app2

    # make sure the URLs are correct
    assert customer_jeff.url == suite.base_url + '/jeff'
    assert customer_jeff['tracked-items'].url == customer_jeff.url + '/tracked-items'


def test_simple_document_create_and_delete(apps):
    customer_jeff, customer_steve = apps
    templates = customer_jeff['tracked-item-templates']
    chint300 = templates.create({
        "name": "CHINT-300.2014",
        "category": "module",
        "baseGeometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
        },
        "properties": {
            "manufacturer": "CHINT",
            "wattage": 300,
        }
    })
    
    assert 'CHINT-300.2014' in templates
    assert isinstance(templates['CHINT-300.2014'], TrackedItemTemplate)
    assert mapping(templates['CHINT-300.2014']['baseGeometry']) == mapping(shape(chint300["baseGeometry"]))
    assert templates['CHINT-300.2014']['properties']['manufacturer'] == 'CHINT'

    chint300.delete()
    assert 'CHINT-300.2014' not in templates


def test_single_reference(tracked_items, tracked_item_templates):
    chint300, ae_inverter = tracked_item_templates
    ae_inverter_0001, chint300_0001, chint300_0002 = tracked_items

    ae_inverter_0001.reference()
    assert ae_inverter_0001['template'] == ae_inverter.url
    assert ae_inverter.url == ae_inverter.collection.url + '/' + ae_inverter.id
    assert ae_inverter.id == ae_inverter['name']

    ae_inverter_0001.dereference()
    assert ae_inverter_0001['template'] == ae_inverter


    
    