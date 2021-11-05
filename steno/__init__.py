from flask import Flask
from . import detail
from . import overview

app = Flask(__name__)

app.register_blueprint(overview.bp)
app.register_blueprint(detail.bp)
