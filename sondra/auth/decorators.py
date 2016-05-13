def authorized_method(o):
    o.authentication_required = o.slug
    o.authorization_required = o.slug
    return o


def authenticated_method(o):
    o.authentication_required = o.slug
    return o


def anonymous_method(o):
    o.authentication_required = False
    o.authorization_required = False
    return o


class authorization_required(object):
    """
    Class decorator for documents, collections, applications that require authorization to access.

    Adds authentication_required and authorization_required attributes to the decorated class at a minimum. It is also
    possible to specify a filter function that filters documents based on a user's authentication information and
    each individual document. This is achieved via rethinkdb's filter API and must use rethinkdb predicates. This should
    be a nested function::

        def example_filter_function(auth_info, method):
            username = auth_info.username
            permission = 'can_' + method
            return lambda(doc): \
                doc[permission].contains(username)

    Args:
        *protected (str): Items should be 'read', 'write', or the name of a method
        filter_function (function): Should be a function that accepts a decoded auth token and an access method, then
            returns another function. The second function should accept a document instance and return True or False
            whether the user has access to that document.
    """
    def __init__(self, *protected, filter_function=None):
        self.protected = protected
        self.filter_function = filter_function

    def __call__(self, cls):
        cls.authentication_required = self.protected
        cls.authorization_required = self.protected
        if self.filter_function:
            cls.document_level_authorization = True
            cls.authorization_filter = self.filter_function
        else:
            cls.document_level_authorization = False

        return cls


class authentication_required(object):
    def __init__(self, *protected):
        self.protected = protected

    def __call__(self, cls):
        cls.authentication_required = self.protected
        return cls
