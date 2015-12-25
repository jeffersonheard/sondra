from datetime import datetime
from slugify import slugify


class DocumentProcessor(object):
    """Modify a document based on a condition, such as before it's saved or when a property changes."""

    def is_necessary(self, changed_props):
        """Override this method to determine whether the processor should run."""
        return False

    def run_before_save(self):
        return False

    def run(self, document):
        """Override this method to post-process a document after it has changed."""
        return document


class SlugPropertyProcessor(DocumentProcessor):
    """Slugify a document property"""

    def __init__(self, source_prop, dest_prop='slug'):
        self.dest_prop = dest_prop
        self.source_prop = source_prop

    def is_necessary(self, changed_props):
        return self.source_prop in changed_props

    def run(self, document):
        document[self.dest_prop] = slugify(document[self.source_prop])


class TimestampOnUpdate(DocumentProcessor):
    """Stamp a document when it's saved"""

    def __init__(self, dest_prop='timestamp'):
        self.dest_prop = dest_prop

    def run_before_save(self):
        return True

    def run(self, document):
        self.dest_prop = datetime.utcnow()


class TimestampOnCreate(DocumentProcessor):
    """Stamp a document when it's created"""

    def __init__(self, dest_prop='created'):
        self.dest_prop = dest_prop

    def run_before_save(self):
        return True

    def run(self, document):
        self.dest_prop = datetime.utcnow()

