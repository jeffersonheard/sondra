from collections import OrderedDict
from datetime import datetime
from slugify import slugify


class DocumentProcessor(object):
    """Modify a document based on a condition, such as before it's saved or when a property changes."""

    def is_necessary(self, changed_props):
        """Override this method to determine whether the processor should run."""
        return True

    def run_after_set(self, document, *changed_props):
        pass

    def run_before_save(self, document):
        pass

    def run_before_delete(self, document):
        pass

    def run_on_constructor(self, document):
        pass

    def run(self, document):
        """
        Override this method to post-process a document after it has changed.

        Args:
            document: the document instance that is being processed.
        """
        pass


class CascadingDelete(DocumentProcessor):
    def __init__(self, app, coll, related_key=None):
        self.app = app
        self.coll = coll
        self.related_key = related_key

    def run_before_delete(self, document):
        self.run(document)

    def run(self, document):
        document.rel(self.app, self.coll, related_key=self.related_key).delete()()


class CascadingOperation(DocumentProcessor):
    def __init__(self, operation, changed_properties, app, coll, related_key=None):
        self.changed_properties = changed_properties
        self.operation = operation
        self.app = app
        self.coll = coll
        self.related_key = related_key

    def run_after_set(self, document, *changed_props):
        if any([p in self.changed_properties for p in changed_props]):
            self.run(document)

    def run(self, document):
        for related_doc in document.rel(self.app, self.coll, related_key=self.related_key).delete():
            self.operation(related_doc, document)


def join(delimiter=','):
    """Returns a function that joins all the passed in properties with the given delimiter."""
    return lambda doc: delimiter.join(str(v) for v in doc.values())


def _prune(d):
    for k, v in d.items():
        if v is None:
            del d[k]
    return d


class DeferredDefaults(DocumentProcessor):
    """
    Set defaults for properties where the default value is not valid JSON schema.
    """
    def __init__(self, **defaults):
        self.defaults = defaults

    def run_on_constructor(self, document):
        for k, default_value in self.defaults.items():
            if k not in document:
                if callable(default_value):
                    document[k] = default_value(document)
                else:
                    document[k] = default_value


class DerivedProperty(DocumentProcessor):
    """
    Derive a property value based on other property values.


    Args:
        dest_prop: the destination property
        required_source_props: the source properties that must be present
        optional_source_props: the source properties that may be None
        derivation: a lambda that receives all
    """

    def __init__(self, dest_prop, required_source_props=None, optional_source_props=None, modify_existing=True, derivation=join(',')):
        self.dest_prop = dest_prop
        self.required_source_props = tuple(required_source_props) if required_source_props else None
        self.optional_source_props = tuple(optional_source_props) if optional_source_props else None
        self.source_props = (required_source_props or ()) + (optional_source_props or ())
        self.modify_existing = modify_existing
        self.derivation = derivation

    def run_on_constructor(self, document):
        if self.modify_existing or not self.dest_prop in document:
            if not self.source_props or all([p in document for p in self.required_source_props]):
                self.run(document)

    def run_after_set(self, document, *changed_props):
        if self.modify_existing or not self.dest_prop in document:
            if not self.source_props or (
                        any([p in changed_props for p in self.source_props]) and
                        all([p in document for p in self.required_source_props])):
                self.run(document)

    def run(self, document):
        if self.source_props:
            document[self.dest_prop] = self.derivation(
                _prune(OrderedDict([(k, document.get(k, None)) for k in self.source_props])))
        else:
            document[self.dest_prop] = self.derivation(document)


class SlugPropertyProcessor(DerivedProperty):
    """Slugify a document property"""

    def op(self, doc):
        return '-'.join([slugify(str(doc[p])) for p in self.source_props])

    def __init__(self, *source_props, dest_prop='slug'):
        super(SlugPropertyProcessor, self).__init__(
            dest_prop,
            source_props,
            None,
            False,
            self.op
        )

    def is_necessary(self, changed_props):
        return any([p in set(changed_props) for p in self.source_props])

    def run_on_constructor(self, document):
        if (document.get(self.dest_prop, None) is None) and all([p in document for p in self.source_props]):
            self.run(document)

    def run_after_set(self, document, *changed_props):
        if (document.get(self.dest_prop, None) is None) and self.is_necessary(changed_props):
            self.run(document)


class TimestampOnUpdate(DocumentProcessor):
    """Stamp a document when it's saved"""

    def __init__(self, dest_prop='timestamp'):
        self.dest_prop = dest_prop

    def run_on_constructor(self, document):
        if self.dest_prop not in document:
            self.run(document)

    def run_before_save(self, document):
        self.run(document)

    def run(self, document):
        document[self.dest_prop] = datetime.utcnow()


class TimestampOnCreate(DocumentProcessor):
    """Stamp a document when it's created"""

    def __init__(self, dest_prop='created'):
        self.dest_prop = dest_prop

    def run_before_save(self, document):
        if not document.saved:
            self.run(document)

    def run(self, document):
        document[self.dest_prop] = datetime.utcnow()
