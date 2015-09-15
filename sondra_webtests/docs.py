from sondra import document, suite, collection, application

from sondra.decorators import expose

class ConcreteSuite(suite.Suite):
    base_url = "http://localhost:5000/api"


class TrackedItemTemplate(document.Document):
    """An item to track within an asset.
    """

    schema = {
        "type": "object",
        "description": "Tracked item",
        "required": ["name"],
        "properties": {
            "name": {"type": "string", "description": "The template name. Must be unique in the collection"},
            "category": {"type": "string", "description": "The category this template belongs to, such as 'module'"},
            "baseGeometry": {"type": "object", "description": "GeoJSON geometry object"},
            "properties": {"type": "object", "description": "extra properties to set on the module, like manufacturer"},
        }
    }

    @expose
    def test_document_method(self, int_arg: int=0, str_arg: str='', list_arg: list=[], dict_arg: dict={}) -> dict:
        return {
            "int_arg": int_arg,
            "str_arg": str_arg,
            "list_arg": list_arg,
            "dict_arg": dict_arg
        }


class OtherApplication(application.Application):
    pass

class BaseApp(application.Application):
    @expose
    def test_app_method(self, int_arg: int=0, str_arg: str='', list_arg: list=[], dict_arg: dict={}) -> dict:
        return {
            "int_arg": int_arg,
            "str_arg": str_arg,
            "list_arg": list_arg,
            "dict_arg": dict_arg
        }

class TrackedItemTemplates(collection.Collection):
    document_class = TrackedItemTemplate
    application = BaseApp
    primary_key = 'name'
    specials = {
        'baseGeometry': document.Geometry()
    }

    @expose
    def test_collection_method(self, int_arg: int=0, str_arg: str='', list_arg: list=[], dict_arg: dict={}) -> dict:
        return {
           "int_arg": int_arg,
            "str_arg": str_arg,
            "list_arg": list_arg,
            "dict_arg": dict_arg
        }


class TrackedItem(document.Document):
    schema = {
        "type": "object",
        "required": ["barcode", "template"],
        "properties": {
            "barcode": {"type": "string"},
            "template": {"type": "string"},
            "location": {"type": "object"},
            "properties": {"type": "object"}
        }
    }


class TrackedItems(collection.Collection):
    document_class = TrackedItem
    application = BaseApp
    primary_key = "barcode"
    specials = {
        "location": document.Geometry("Point")
    }
    relations = [("template", TrackedItemTemplates)]
    indexes = ("template","location")


class TrackedItemHistory(document.Document):
    schema = {
        "type": "object",
        "properties": {
            "item": {"type": "string"},
            "location": {"type": "object"},
            "timestamp": {"type": "string"},
            "properties": {"type": "object"}
        }
    }


class TrackedItemHistories(collection.Collection):
    document_class = TrackedItemHistory
    application = BaseApp
    specials = {
        "location": document.Geometry("Point"),
        "timestamp": document.Time(),
    }
    references = [("item", TrackedItem)]
    indexes = ("item", "location", "timestamp")


class ItemGroup(document.Document):
    schema = {
        "type": "object",
        "required": ["name", "items"],
        "properties": {
            "parent": {"type": "string"},
            "name": {"type": "string"},
            "geometry": {"type": "object"},
            "items": {
                "type": "array",
                "items": {"type": "string"}
            }
        }
    }


class ItemGroups(collection.Collection):
    document_class = ItemGroup
    application = BaseApp
    specials = {
        "geometry": document.Geometry('Polygon')
    }
    references = [("parent", "self"), {"items": TrackedItems}]