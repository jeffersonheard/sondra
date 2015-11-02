from flask import request, Blueprint, current_app, Response
import json

from .api import APIRequest

api_tree = Blueprint('api', __name__)

@api_tree.route('/schema')
@api_tree.route(';schema')
def suite_schema():
    resp = Response(
        json.dumps(current_app.suite.schema, indent=4),
        status=200,
        mimetype='application/json'
    )
    if current_app.suite.cross_origin:
        resp.headers['Access-Control-Allow-Origin'] = '*'

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
    if current_app.suite.cross_origin:
        resp.headers['Access-Control-Allow-Origin'] = '*'

    return resp

@api_tree.route('/<path:path>', methods=['GET','POST','PUT','PATCH', 'DELETE'])
def api_request(path):
    args = dict(request.args)
    if 'q' not in args:
        if request.form:
            args['q'] = [json.dumps({k: v for k, v in request.form})]

    r = APIRequest(
        current_app.suite,
        request.headers,
        request.data,
        request.method,
        None,
        current_app.suite.base_url + '/' + path,
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

    if current_app.suite.cross_origin:
        resp.headers['Access-Control-Allow-Origin'] = '*'

    return resp