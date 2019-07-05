import json

import flask
from flask import current_app as app
from flask import render_template

from . import indicator

bp = flask.Blueprint('view', __name__, url_prefix='/')


@bp.route('/')
def index():
    return render_template('view/index.html')


@bp.route('/search', methods=['POST'])
def search():
    try:
        location = json.loads(flask.request.form['location'])
        latitude = location['lat']
        longitude = location['lng']
    except (json.JSONDecodeError, KeyError):
        return flask.abort(400)

    city = app.azavea.get_nearest_city(latitude, longitude)
    if city:
        with app.app_context():
            top = indicator.get_top_indicators(city)

        results = '\n'.join(f'{i.label}: {i.rate}' for i in top)
    else:
        flask.flash('Location not found.')
        results = None

    return render_template('view/index.html', city=str(city), results=results)
