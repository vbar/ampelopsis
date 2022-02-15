from flask import Flask
from . import detail
from . import overview
from . import palette
from . import summary

app = Flask(__name__)

app.register_blueprint(detail.bp)
app.register_blueprint(overview.bp)
app.register_blueprint(palette.bp)
app.register_blueprint(summary.bp)
