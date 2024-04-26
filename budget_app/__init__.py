import os
import calendar
from datetime import datetime
from flask import Flask

def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        # DATABASE=os.path.join(app.instance_path, 'budget_app.postgreSQL'),
        DATABASE='budget_app',
        HOST='localhost',
        ALLOWED_EXTENSIONS = {'txt', 'xlsx', 'csv', 'pdf'},
        UPLOAD_FOLDER = 'budget_app/data_pipeline/data_in',
        OUTPUT_FOLDER = 'budget_app/data_pipeline/data_out',
        FIRST_OF_MONTH = datetime.now().replace(day=1).strftime("%Y-%m-%d"),
        LAST_OF_MONTH = datetime(datetime.now().year, datetime.now().month, calendar.monthrange(datetime.now().year, datetime.now().month)[1]).strftime("%Y-%m-%d")
    )

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    
    @app.route('/')
    def test():
        return 'test test test'

    # register the authorization blueprint 
    from . import auth
    app.register_blueprint(auth.bp)

    # register the db blueprint with the app upon app fire-up
    from . import init_db
    init_db.init_app(app)

    # register transactions bp
    from . import transactions
    app.register_blueprint(transactions.bp)

    return app