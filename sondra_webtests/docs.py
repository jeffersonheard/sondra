from sondra import document
from sondra.decorators import expose

# WHy do we do this?  We need to test that the environment created by the Environment constructor is
# ConcreteSuite() and not any of the base classes.  This tests the EnvironmentMetaclass
class BaseSuite(document.Suite):
    pass

class BaseSuite2(document.Suite):
    pass

class ConcreteSuite(BaseSuite, BaseSuite2):
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


class OtherApplication(document.Application):
    pass

class BaseApp(document.Application):
    @expose
    def test_app_method(self, int_arg: int=0, str_arg: str='', list_arg: list=[], dict_arg: dict={}) -> dict:
        return {
            "int_arg": int_arg,
            "str_arg": str_arg,
            "list_arg": list_arg,
            "dict_arg": dict_arg
        }

class TrackedItemTemplates(document.Collection):
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
        "properties": {
            "barcode": {"type": "string"},
            "template": {"type": "string"},
            "location": {"type": "object"},
            "properties": {"type": "object"}
        }
    }


class TrackedItems(document.Collection):
    document_class = TrackedItem
    application = BaseApp
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
    application = BaseApp
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
    application = BaseApp
    specials = {
        "geometry": document.Geometry('polygon')
    }
    references = [("parent", "self"), {"items": TrackedItems}]