"""Tools for creating JSON Schemas in Sondra.

This module contains functions for making schemas a bit simpler and more foolproof to create. These include functions
to create references between the schemas of Collections or Applications.
"""

import importlib
from functools import partial


def _deferred_url_for(klass, context, fmt='schema', fragment=None):
    from sondra.document import Document
    from sondra.suite import Suite
    from sondra.application import Application
    from sondra.collection import Collection

    if isinstance(klass, str):
        modulename, classname = klass.rsplit(klass, 1)
        klass = getattr(importlib.import_module(modulename), classname)

    slug = klass.slug

    if isinstance(context, Suite):
        suite = context
    elif isinstance(context, Application):
        suite = context.suite
    elif isinstance(context, Collection):
        suite = context.application.suite
    elif isinstance(context, Document):
        suite = context.collection.application.suite
    elif context is None:
        suite = context
    else:
        raise ValueError("Context must be an instance of Application, Document, Collection, Suite, or None")

    ret = ""
    if issubclass(klass, Document):
        for app in suite.applications.values():
            for coll in app.collections.values():
                if coll.document_class is klass:
                    ret = coll.url + ';' + fmt
                    break
            else:
                raise KeyError("Cannot find document in a registered application")
    elif issubclass(klass, Collection):
        for app in suite.applications.values():
            if slug in app:
                ret = app[slug].url
                break
        else:
            raise KeyError("Cannot find collection in a registered application")
    elif issubclass(klass, Application):
        ret = suite[slug].url + ';' + fmt
    elif issubclass(klass, Suite):
        ret = suite.url
    else:
        raise ValueError("Target class must be an Application, Document, Collection, or Suite")

    if fragment:
        return ret + "#" + fragment
    else:
        return ret


def url_for(klass, fmt='schema'):
    """Defer the calculation of the URL until the application has been initialized.

    Args:
        klass (str or type): The class whose URL to search for. Must be a Collection, Application, or Suite.
        format (str): The "format" portion of the API call. By default this is schema.

    Returns:
        callable: A function that takes a context. The context is an *instance* of Collection, Application, Document, or
            Suite. Optionally, the context can also be None, in which case the class's ``slug`` is returned.
    """
    return partial(_deferred_url_for, klass=klass, fmt=fmt)


def lazy_refers_to(klass, **kwargs):
    """
    Create a schema fragment that lazily refers to the schema of a Collection, Application, or Suite.

    Args:
        klass (str or type): The class whose URL to search for. Must be a Collection, Application, or Suite.
        **kwargs: Additional properties to set on the schema fragment, often ``"description"``.

    Returns:
        dict: A schema fragment containing a callable returned by :py:func:`url_for`
    """
    ret = {"type": "string", "refersTo": url_for(klass)}
    if kwargs:
        ret.update(kwargs)
    return ret
