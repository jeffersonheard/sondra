import json
from flask import Flask, request, current_app, Response

from sondra.flask import api_tree
from sondra.document import Suite

import docs

app = Flask(__name__)
app.debug = True

app.suite = Suite()
api = docs.BaseApp()
api.create_database()
api.create_tables()

app.register_blueprint(api_tree, url_prefix='/api')

if __name__ == '__main__':
    app.run()