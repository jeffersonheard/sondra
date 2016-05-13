from flask import request, Blueprint, current_app, Response, abort
from flask.ext.cors import CORS

import json
import traceback

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

@api_tree.route('/<path:path>', methods=['GET','POST','PUT','PATCH', 'DELETE'])
def api_request(path):
    if request.method == 'HEAD':
        return Response(status=200)
    else:
        args = {k:v for k, v in request.values.items()}

        try:
            r = APIRequest(
                current_app.suite,
                request.headers,
                request.data,
                request.method,
                current_app.suite.url + '/' + path,
                args,
                request.files
            )

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
            return Response(
                status=403,
                mimetype='application/json',
                response=json.dumps({"err": "PermissionDenied", "reason": str(denial)})
            )

        except KeyError as not_found:
            return Response(
                status=404,
                mimetype='application/json',
                response=json.dumps({"err": "NotFound", "reason": str(not_found)}))

        except ValidationError as invalid_entry:
            return Response(
                status=400,
                mimetype='application/json',
                response=json.dumps({
                    "err": "InvalidRequest",
                    "reason": str(invalid_entry),
                    "request_data": request.data.decode('utf-8'),
                    "request_path": current_app.suite.url + '/' + path,
                    "method": request.method,
                    "args": args
                }, indent=4)
            )

        except Exception as error:
            return Response(
                status=500,
                mimetype='application/json',
                response=json.dumps({
                    "err": error.__class__.__name__,
                    "reason": str(error),
                    "traceback": traceback.format_exc()
                }, indent=4))


