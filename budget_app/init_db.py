import os
import psycopg2
import click
from flask import g, current_app


# Connect to the db
def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(database = current_app.config['DATABASE'], host = current_app.config['HOST'])
    return g.db

# close the connection to the db after performing CRUD
def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

# Initialize the db
def init_db():
    db = get_db()
    with current_app.open_resource('schema.sql') as f:
        db.cursor().execute(f.read().decode('utf8'))
        # all connections needs to be "commmitted"
        g.db.commit()

@click.command('init-db')
def init_db_command():
    init_db()
    click.echo('The db has been initialized')

# The close_db and init_db_command functions need to be registered with the application instance; otherwise, they won’t be used by the application
def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
