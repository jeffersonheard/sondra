import pytest

from sondra import document, suite, collection, application
from shapely.geometry import mapping, shape
import sondra.collection


class ConcreteSuite(suite.Suite):
    pass


class SimpleDocument(document.Document):
    schema = {
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": {"type": "string", "description": "The template name. Must be unique in the collection"},

        }
    }


class SimpleDocuments(collection.Collection):
    document_class = SimpleDocument
    primary_key = 'name'


class BaseApp(application.Application):
    collections = (
        SimpleDocument,
    )


@pytest.fixture(scope='module')
def s(request):
    v = ConcreteSuite()
    return v

@pytest.fixture(scope='module')
def apps(request, s):
    customer_jeff = TerraHubBase(s, 'jeff')
    customer_steve = TerraHubBase(s, 'steve')
    s.clear_databases()
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