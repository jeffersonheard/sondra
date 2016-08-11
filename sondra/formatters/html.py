import json

from sondra import document
from sondra.utils import mapjson
from sondra.ref import Reference
from io import StringIO
from json2html import json2html

class HTML(object):
    """
    This formats the API output as HTML. Used when ;formatters=json or ;json is a parameter on the last item of a URL.

    Optional arguments:

    * **indent** (int) - Formats the JSON output for human reading by inserting newlines and indenting ``indent`` spaces.
    * **fetch** (string) - A key in the document. Fetches the sub-document(s) associated with that key.
    * **ordered** (bool) - Sorts the keys in dictionary order.
    * **bare_keys** (bool) - Sends bare foreign keys instead of URLs.
    """
    # TODO make dotted keys work in the fetch parameter.

    def format(self, structure, buf=None, wrap=True):
        base = buf is None
        buf = buf or StringIO()

        if base and wrap:
            buf.write("""<!doctype html>
<html>
<head>
<!-- Latest compiled and minified CSS -->
<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">

<!-- Optional theme -->
<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css">

</head><body>
<!-- Latest compiled and minified JavaScript -->
<script src="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
""")

        if isinstance(structure, list):
            buf.write('<ol>')
            for x in structure:
                buf.write('<li>')
                self.format(x, buf=buf)
                buf.write('</li>')
            buf.write('</ol>')
        elif isinstance(structure, dict):
            buf.write('<dl>')
            for k, v in structure.items():
                if isinstance(v, dict):
                    buf.write("<dt>{0}</dt>".format(k))
                    buf.write('<dl>')
                    for a, b in v.items():
                        buf.write("<dt>{0}</dt>".format(a))
                        buf.write("<dd>")
                        self.format(b, buf=buf)
                        buf.write("</dd>")
                    buf.write('</dl>')

                elif isinstance(v, list):
                    buf.write("<dt>{0}</dt>".format(k))
                    buf.write('<dd><ol>')
                    for x in v:
                        buf.write('<li>')
                        self.format(x, buf=buf)
                        buf.write('</li>')
                    buf.write('</ol></dd>')

                elif base:
                    buf.write('<dl><dt>{0}</dt><dd>{1}</dd></dl>'.format(k, v))

                else:
                    buf.write("<dt>{0}</dt>".format(k))
                    buf.write("<dd>({1}) {0}</dd>".format(v, str(v.__class__.__name__)))
            buf.write("</dl>")
        else:
            buf.write(str(structure))

        if base and wrap:
            buf.write("</body></html>")
            return buf.getvalue()


    def __call__(self, reference, results, **kwargs):

        # handle indent the same way python's json library does
        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        if 'ordered' in kwargs:
            ordered = bool(kwargs.get('ordered', False))
            del kwargs['ordered']
        else:
            ordered = False

        # fetch a foreign key reference and append it as if it were part of the document.
        if 'fetch' in kwargs:
            fetch = kwargs['fetch'].split(',')
            del kwargs['fetch']
        else:
            fetch = []

        if 'bare_keys' in kwargs:
            bare_keys = bool(kwargs.get('bare_keys', False))
            del kwargs['bare_keys']
        else:
            bare_keys = False

        # note this is a closure around the fetch parameter. Consider before refactoring out of the method.
        def serialize(doc):
            if isinstance(doc, document.Document):
                ret = doc.json_repr(ordered=ordered, bare_keys=bare_keys)
                for f in fetch:
                    if f in ret:
                        if isinstance(doc[f], list):
                            ret[f] = [d.json_repr(ordered=ordered, bare_keys=bare_keys) for d in doc[f]]
                        elif isinstance(doc[f], dict):
                            ret[f] = {k: v.json_repr(ordered=ordered, bare_keys=bare_keys) for k, v in doc[f].items()}
                        else:
                            ret[f] = doc[f].json_repr(ordered=ordered, bare_keys=bare_keys)
                return ret
            else:
                return doc

        result = mapjson(serialize, results)  # make sure to serialize a full Document structure if we have one.

        if not (isinstance(result, dict) or isinstance(result, list)):
            result = {"_": result}

        rsp = StringIO()
        rsp.write("""<!doctype html>
<html>
<head>
<!-- Latest compiled and minified CSS -->
<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">

<!-- Optional theme -->
<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css">

</head><body>
<!-- Latest compiled and minified JavaScript -->
<script src="//maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
""")
        rsp.write(json2html.convert(json=result, table_attributes="class=\"table table-bordered table-hover\""))
        rsp.write('</body></html>')
        return 'text/html',rsp.getvalue()