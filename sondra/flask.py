from flask import request, Blueprint, current_app, Response, abort
from flask.ext.cors import CORS

import json
import traceback
import sys

from jsonschema import ValidationError

from .api import APIRequest

api_tree = Blueprint('api', __name__)

def init(app):
    if hasattr(app.suite, 'max_content_length'):
        app.config['MAX_CONTENT_LENGTH'] = app.suite.max_content_length
    if app.suite.cross_origin:
        CORS(api_tree, intercept_exceptions=True)


@api_tree.route('/schema')
@api_tree.route(';schema')
def suite_schema():
    resp = Response(
        json.dumps(current_app.suite.schema, indent=4),
        status=200,
        mimetype='application/json'
    )
    return resp

@api_tree.route('/help')
@api_tree.route(';help')
def suite_help():
    h = current_app.suite.help()
    help_text = current_app.suite.docstring_processor(h)

    resp = Response(
        help_text,
        status=200,
        mimetype='text/html'
    )
    return resp

def format_error(req, code, err, reason):
    if isinstance(reason, Exception):
        kind, value, tb = sys.exc_info()
        reason = "{kind}: {value}\n------\n\n{tb}".format(
            kind=reason.__class__.__name__,
            value=value,
            tb='\n'.join(traceback.format_tb(tb, limit=100))
        )
    else:
        reason = str(reason)

    try:
        if req.reference.format == 'json':
            return Response(
                status=code,
                mimetype='application/json',
                response=json.dumps({"err": err, "reason": str(reason)})
            )
        else:
            rsp = """<!doctype html><html>
            <head>
                <title>{code} - {url}</title>
            </head>
            <body>
                <h1>{code} - {err}</h1>
                <dl>
                    <dt>URL</dt>
                    <dd>{url}</dd>

                    <dt>Method</dt>
                    <dd>{method}</dd>

                </dl>
                <h3>Reason</h3>
                <pre>
    {reason}
                </pre>
            </body>
    </html>""".format(
                code=code,
                url=req.reference.url,
                err=err,
                method=req.request_method,
                reason=reason
            )
            return Response(
                status=code,
                mimetype='text/html',
                response=rsp
            )
    except:
        return Response(
            status=code,
            mimetype='application/json',
            response=json.dumps({
                "err": "InvalidRequest",
                "reason": reason,
                "request_data": req.body.decode('utf-8'),
                "request_path": req.reference.url,
                "method": req.method,
            }, indent=4)
        )


@api_tree.route('/<path:path>', methods=['GET','POST','PUT','PATCH', 'DELETE'])
def api_request(path):
    if request.method == 'HEAD':
        return Response(status=200)
    else:
        args = {k:v for k, v in request.values.items()}
        r = APIRequest(
                current_app.suite,
                request.headers,
                request.data,
                request.method,
                current_app.suite.url + '/' + path,
                args,
                request.files
            )

        try:
            # Run any number of post-processing steps on this request, including
            try:
                for p in current_app.suite.api_request_processors:
                    r = p(r)
            except Exception as e:
                for p in current_app.suite.api_request_processors:
                    p.cleanup_after_exception(r, e)
                raise e

            r.validate()

            mimetype, response = r()
            resp = Response(
                response=response,
                status=200,
                mimetype=mimetype)
            return resp

        except PermissionError as denial:
            return format_error(r, 403, "PermissionDenied", denial)

        except KeyError as not_found:
            return format_error(r, 404, "NotFound", not_found)

        except ValidationError as invalid_entry:
            return format_error(r, 400, "InvalidRequest", invalid_entry)

        except Exception as error:
            return format_error(r, 500, "ServerError", error)


