from examples.todo import *

from flask import Flask
from sondra.flask import api_tree, init

# Create the Flask instance and the suite.
app = Flask(__name__)
app.debug = True
app.suite = TodoSuite()
init(app)

# Register all the applications.
TodoApp(app.suite)

# Create all databases and tables.
app.suite.validate()  # remember this call?
app.suite.ensure_database_objects()  # and this one?

# Attach the API to the /api/ endpoint.
app.register_blueprint(api_tree, url_prefix='/api')

app.run()