from urllib.parse import urlencode, urlparse, parse_qs

from sondra.utils import is_exposed

from sondra.api.expose import method_schema


class ParseError(Exception):
    """Called when an API request is not parsed as valid"""


class EndpointError(Exception):
    """Raised when an API request is parsed correctly, but the endpoint isn't found"""


class Reference(object):
    """Contains the application, collection, document, methods, and fragment the URL refers to"""
    FORMATS = {'help', 'schema', 'json', 'geojson', 'html'}

    def __str__(self):
        return self.url

    def __init__(self, env, url=None, **kw):
        self.environment = env

        if url and (url.endswith('/') or url.endswith("?")):  # strip trailing slash
            url = url[:-1]

        self.url = url
        self.app = kw.get("app")
        self.app_method = kw.get("app_method")
        self.coll = kw.get("coll")
        self.coll_method = kw.get("coll_method")
        self.doc = kw.get("doc")
        self.doc_method = kw.get("doc_method")
        self.fragment = kw.get("fragment")
        self.format = format = kw.get("format", 'json')
        self.query = query = kw.get("query")
        self.vargs = vargs = kw.get("vargs", [])
        self.kwargs = kwargs = kw.get("kwargs", {})

        if not url:
            url = self.construct()
            self.url = url

        # to allow browsers to pass fragments to the server, change fragment character
        p_url = urlparse(url.replace("@!", "#"))

        # if this is a relative URL, pass it through. Otherwise make sure that the base URL is the same.
        if not all((
            (not p_url.scheme) or p_url.scheme == self.environment.base_url_scheme,
            (not p_url.netloc) or p_url.netloc == self.environment.base_url_netloc,
            (not self.environment.base_url_path) or p_url.path.startswith(self.environment.base_url_path)
        )):
            raise EndpointError("{0} does not refer to the application hosted at {1}".format(
                                url, self.environment.url))

        # make sure future references to the url are absolute.
        if not p_url.netloc:
            self.url = self.environment.url + self.url

        # fix the path if our applications are at an offset
        path = p_url.path if not self.environment.base_url_path\
            else p_url.path[len(self.environment.base_url_path):]

        if path.endswith('/'):
            path = path[:-1]
        if path.startswith('/'):
            path = path[1:]

        app, *rest = path.split('/')
        app_method = None
        coll = None
        coll_method = None
        doc = None
        doc_method = None

        # determine if there's a collection or a doc
        if len(rest) == 2:
            coll, doc = rest
        elif len(rest) == 1:
            coll = rest[0]
        #else:
        #    coll = None
        #    doc = None

        # parse out method names
        if '.' in app:
            app, app_method = app.split('.', 1)
            app_method = app_method.replace('-', '_')

        if coll and '.' in coll:
            coll, coll_method = coll.split('.', 1)
            coll_method = coll_method.replace('-', '_')

        if doc and '.' in doc:
            doc, doc_method = doc.split('.', 1)
            doc_method = doc_method.replace('-', '_')

        if coll and (coll not in self.environment[app]):
            raise EndpointError('{0} not found in {1}'.format(url, self.environment.url))

        # parse out the fragment portion, which refers to a subdocument inside the URL
        fragment = p_url.fragment.split('/') if p_url.fragment else None
        if fragment:
            fragment[-1], *fragment_method = fragment[-1].split('.')
            fragment = tuple(fragment)

        # parse params
        if p_url.params:
            params = p_url.params.split(';')
            vargs = [a for a in params if '=' not in a]
            kwargs = {k: v for k, v in (kv.split('=') for kv in params if '=' in kv)}

        # determine output formatters
        if vargs and 'format' not in kwargs:
            format = vargs[0]
        elif 'format' in kwargs:
            format = kwargs['format']

        if format not in self.FORMATS:
            raise ParseError('Unknown output format: {0}'.format(format))

        # check basic validity of URL
        if app_method and any((coll, coll_method, doc, doc_method)):
            raise ParseError("App method specified with collection or doc")

        if coll_method and any((doc, doc_method)):
            raise ParseError("Collection method specified with app method, document, or doc method")

        # get query parameters
        if p_url.query:
            query = parse_qs(p_url.query)

        self.url = url
        self.app = app
        self.app_method = app_method
        self.coll = coll
        self.coll_method = coll_method
        self.doc = doc
        self.doc_method = doc_method
        self.fragment = fragment
        self.vargs = vargs
        self.kwargs = kwargs
        self.format = format
        self.query = query

    @classmethod
    def maybe_reference(cls, sondra, value):
        if '/' not in value:
            return value, False

        try:
            return cls(sondra, value), True
        except:
            return value, False

    @classmethod
    def dereference(cls, sondra, value):
        if not isinstance(value, str):
            return value

        val, is_reference = cls.maybe_reference(sondra, value)
        if is_reference:
            return val.value
        else:
            return val

    @property
    def value(self):
        if self.is_application():
            return self.get_application()
        elif self.is_collection():
            return self.get_collection()
        elif self.is_document():
            return self.get_document()
        elif self.is_subdocument():
            subd = self.get_subdocument()[-1]
            return Reference.dereference(self.environment, subd)
        elif self.is_application_method_call():
            return (self.get_application(), self.get_application_method())
        elif self.is_collection_method_call():
            return (self.get_collection(), self.get_collection_method())
        elif self.is_document_method_call():
            return (self.get_document(), self.get_document_method())
        else:
            raise EndpointError("Endpoint {0} cannot be dereferenced. This is likely a bug.".format(self.url))

    @property
    def schema(self):
        if self.is_application():
            return self.get_application().schema
        elif self.is_collection():
            return self.get_collection().schema
        elif self.is_document():
            return self.get_document().collection.schema
        elif self.is_subdocument():
            return self.get_document().collection.schema
        elif self.is_application_method_call():
            return method_schema(self.get_application(), self.get_application_method())
        elif self.is_collection_method_call():
            return method_schema(self.get_collection(), self.get_collection_method())
        elif self.is_document_method_call():
            return method_schema(self.get_document(), self.get_document_method())
        else:
            raise EndpointError("Endpoint {0} cannot be dereferenced. This is likely a bug.".format(self.url))

    @classmethod
    def dereference_all(cls, sondra, value):
        val = cls.dereference(sondra, value)
        if isinstance(val, dict):
            return {k: cls.dereference_all(sondra, v) for k, v in val.items()}
        elif isinstance(val, list):
            return [cls.dereference_all(sondra, v) for v in val]
        else:
            return val

    def construct(self):
        """Construct a URL from its base parts"""

        url = self.environment.url + '/' + self.app
        if self.app_method:
            url += '.' + self.app_method
        if self.coll:
            url += '/' + '/'.join(self.coll)
        if self.coll_method:
            url += '.' + self.coll_method
        if self.doc:
            url += '/' + self.doc
        if self.doc_method:
            url += '.' + self.doc_method
        url += ';' + self.format
        if self.vargs:
            url += ';' + ';'.join(str(a) for a in self.vargs)
        if self.kwargs:
            url += ';' + ';'.join("{0}={1}".format(k, v) for k, v in self.kwargs.items())
        url += '/'
        if self.fragment:
            url += "@/" + '/'.join(str(f) for f in self.fragment)
        if self.query:
            url += "?" + urlencode(self.query)

        return url

    def is_collection(self):
        return self.coll and not any((self.doc, self.coll_method))

    def is_application(self):
        return self.app and not any((self.doc, self.coll, self.coll_method, self.app_method))

    def is_document(self):
        return self.doc and not any((self.fragment, self.doc_method))

    def is_subdocument(self):
        return self.doc and self.fragment

    def is_application_method_call(self):
        return self.app_method is not None

    def is_document_method_call(self):
        return self.doc_method is not None

    def is_collection_method_call(self):
        return self.coll_method is not None

    @property
    def kind(self):
        if self.is_document_method_call():
            return 'document_method'
        elif self.is_collection_method_call():
            return 'collection_method'
        elif self.is_application_method_call():
            return 'application_method'
        elif self.is_subdocument():
            return 'subdocument'
        elif self.is_document():
            return 'document'
        elif self.is_collection():
            return 'collection'
        elif self.is_application():
            return 'application'
        else:
            raise Exception("Cannot determine kind of reference. This is probably a bug.")

    def get_application(self):
        """Return an Application instance for the given URL.

        Args:
            url (str): The URL to a collection or a document.

        Returns:
            An Application object
        """
        try:
            return self.environment[self.app]
        except KeyError:
            raise EndpointError("{0} does not refer to an application.".format(self.url))

    def get_collection(self):
        """Return a DocumentCollection instance for the given URL.

        Args:
            url (str): The URL to a collection or a document.

        Returns:
            A DocumentCollection object
        """
        try:
            return self.environment[self.app][self.coll]
        except KeyError:
            raise EndpointError("{0} does not refer to a collection.".format(self.url))

    def get_document(self):
        """Return the Document for a given URL."""
        if self.doc == '*':
            return None

        try:
            return self.get_collection()[self.doc]
        except KeyError as e:
            raise EndpointError("{0} document not found.\n{1}".format(self.url, e))

    def get_subdocument(self):
        """Return the fragment within the Document referred to by this URL."""
        from sondra.document import Document
        path = self.fragment
        frag = parent = d = self.get_document()
        frag.collection.application.dereference(frag)
        walk = []

        try:
            while path:
                if isinstance(frag, Document):
                    parent = frag
                    walk = []
                key = path.pop(0)
                walk.append(key)
                frag = frag[key]

        except KeyError:
            raise EndpointError("{0} not found in document".format('/'.join(self.fragment)))

        return d, parent, tuple(walk), frag

    def get_application_method(self):
        """Return everything you need to call an application method.

        Returns:
            A three-tuple of:
                * application object
                * method name
                * method object

        Raises:
            EndpointError if the method or application is not found or the method is not exposable.
        """

        obj = self.get_application()
        method = getattr(obj, self.app_method, None)
        if not method:
            raise EndpointError("{0} is not a method of {1}".format(
                                        self.app_method, self.app))
        if is_exposed(method):
            return method
        else:
            raise EndpointError("{0} is not an exposable method on {1}".format(
                                        self.app_method, self.app))

    def get_collection_method(self):
        """Return everything you need to call an collection method.

        Returns:
            A three-tuple of:
                * collection object
                * method name
                * method object

        Raises:
            EndpointError if the method or collection is not found or the method is not exposable.
        """

        obj = self.get_collection()
        method = getattr(obj, self.coll_method, None)
        if not method:
            raise EndpointError("{0} is not a method of {1}".format(
                                        self.coll_method, self.app))
        if is_exposed(method):
            return method
        else:
            raise EndpointError("{0} is not an exposable method on {1}".format(
                                        self.coll_method, self.app))

    def get_document_method(self):
        """Return everything you need to call an document method.

        Returns:
            A three-tuple of:
                * document object
                * method name
                * method object

        Raises:
            EndpointError if the method or document is not found or the method is not exposable.
        """

        obj = self.get_document()
        if obj is None:
            obj = self.get_collection().document_class

        method = getattr(obj, self.doc_method, None)
        if not method:
            raise EndpointError("{0} is not a method of {1}".format(
                                        self.doc_method, self.app))
        if is_exposed(method):
            return method
        else:
            raise EndpointError("{0} is not an exposable method on {1}".format(
                                        self.doc_method, self.app))
