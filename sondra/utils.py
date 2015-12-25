import importlib
import inspect
import re
from copy import deepcopy


def split_camelcase(name):
    return re.sub('([A-Z]+)', r' \1', name).title().strip()


def convert_camelcase(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def camelcase_slugify(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()


def mapjson(fun, doc):
    if isinstance(doc, dict):
        return {k: mapjson(fun, v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [mapjson(fun, v) for v in doc]
    else:
        return fun(doc)


def qiter(o):
    if o is not None:
        for x in o:
            yield x
    else:
        raise StopIteration


def is_exposed(fun):
    return hasattr(fun, 'exposed')


def schema_with_properties(original, **updates):
    new_schema = deepcopy(original)
    new_schema['properties'].update(updates)
    return new_schema


def schema_sans_properties(original, *properties):
    new_schema = deepcopy(original)
    for property in (p for p in properties if p in new_schema['properties']):
        del new_schema['properties'][property]
    return new_schema


def schema_with_definitions(original, **updates):
    new_schema = deepcopy(original)
    new_schema['definitions'].update(updates)
    return new_schema


def schema_sans_definitions(original, *properties):
    new_schema = deepcopy(original)
    for property in (p for p in properties if p in new_schema['definitions']):
        del new_schema['definitions'][property]
    return new_schema


def resolve_class(obj, required_superclass=object, required_metaclass=type):
    if isinstance(obj, str):
        modulename, classname = obj.rsplit('.', 1)
        module = importlib.import_module(modulename)
        klass = getattr(module, classname)
    else:
        klass = obj

    if not issubclass(klass, required_superclass):
        raise TypeError("{0} is not of type {1}".format(
            klass.__name__,
            required_superclass.__name__
        ))

    if not isinstance(klass, required_metaclass):
        raise TypeError("{0} must use {1} metaclass".format(
            klass.__name__,
            required_metaclass.__name__
        ))

    return obj

# if isinstance(attrs['document_class'], str):
#                 modulename, classname = attrs['document_class'].rsplit('.', 1)
#                 module = importlib.import_module(modulename)
#                 klass =