class QWrapper(object):
    """Wraps a rethinkdb query so that we can return instances when we want to and not use the raw interface"""
    def __init__(self, flt, name):
        self.name = name
        self.flt = flt

    def __call__(self, *args, **kwargs):
        res = getattr(self.flt.query, self.name)(*args, **kwargs)
        self.flt.query = res
        return self.flt


class QuerySet(object):
    """Wraps a rethinkdb query so that we can return instances when we want to and not use the raw interface"""
    def __init__(self, coll):
        self.coll = coll
        self.query = self.coll.table
        self.result = None

    def __getattribute__(self, name):
        if name.startswith('__') or name in { 'query', 'coll', 'result', 'first', 'drop', 'pop' }:
            return object.__getattribute__(self, name)
        else:
            return QWrapper(self, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __call__(self):
        return self.coll.q(self.query)

    def __iter__(self):
        return self.coll.q(self.query)

    def __bool__(self):
        return len(self) > 0

    def __len__(self):
        return self.query.count().run(self.coll.application.connection)

    def drop(self):
        """
        Delete documents one at a time for safety and signal processing.
        """
        for d in self:
            d.delete()

    def pop(self):
        """
        Same as drop, but yields each document as part of a generator before deleting.

        Yields:
            A Document object for every document that will be deleted
        """
        for d in self:
            yield d
            d.delete()

    def first(self):
        """
        Return the first result of the query.

        Returns:
            The first result, or None if the query is empty.
        """
        try:
            return next(iter(self))
        except StopIteration:
            return None


class RawQuerySet(object):
    def __init__(self, coll, cls=None):
        self.coll = coll
        self.query = self.coll.table
        self.result = None
        self.cls = cls

    def __getattribute__(self, name):
        if name.startswith('__') or name in { 'query', 'coll', 'result', 'clear', 'cls', 'first'}:
            return object.__getattribute__(self, name)
        else:
            return QWrapper(self, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __call__(self):
        return self.query.run(self.coll.application.connection)

    def __iter__(self):
        if self.cls:
            return (self.cls(d) for d in self.query.run(self.coll.application.connection))
        else:
            return self.query.run(self.coll.application.connection)

    def __bool__(self):
        return len(self) > 0

    def __len__(self):
        return self.query.count().run(self.coll.application.connection)

    def first(self):
        try:
            return next(iter(self))
        except StopIteration:
            return None