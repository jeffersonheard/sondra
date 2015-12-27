from flask import request, Blueprint, current_app, Response, abort
from flask.ext.cors import CORS
import json

from .api import APIRequest

api_tree = Blueprint('api', __name__)

def init(app):
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
        resp = Response(status=200)
    else:
        args = dict(request.args)
        if 'q' not in args:
            if request.form:
                args['q'] = [json.dumps({k: v for k, v in request.form.items()})]

        try:
            r = APIRequest(
                current_app.suite,
                request.headers,
                request.data,
                request.method,
                None,
                current_app.suite.url + '/' + path,
                args,
                request.files
            )

            # Run any number of post-processing steps on this request, including
            for p in current_app.suite.api_request_processors:
                r = p(r)

            mimetype, response = r()
            resp = Response(
                response=response,
                status=200,
                mimetype=mimetype)
            return resp

        except PermissionError as denial:
            abort(403, description=str(denial))

        except KeyError as not_found:
            abort(404, description=str(not_found))

        except Exception as error:
            abort(500, description=str(error))


