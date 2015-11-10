from flask import Flask, send_from_directory

from sondra.auth import Auth
from sondra.flask import api_tree

from sondra.examples.easynote.suite import EasyNoteSuite
from sondra.examples.easynote.application import EasyNote

app = Flask(__name__)
app.debug = True

app.suite = EasyNoteSuite()

# Register all the applications.
auth = Auth(app.suite)
core = EasyNote(app.suite)

# Create all databases and tables.
app.suite.ensure_database_objects()

# Attach the API to the /api/ endpoint.
app.register_blueprint(api_tree, url_prefix='/api')

@app.route('/static/css/<path:path>')
def send_css(path):
    return send_from_directory('/Users/jeff/Dropbox/src/pronto/sondra/sondra/static/css', path)

if __name__ == '__main__':
    app.run()