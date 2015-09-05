import inspect
import re


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

def is_exposed(fun):
    return inspect.ismethod(fun) and hasattr(fun, 'exposed')
