import functools
from psycopg2 import IntegrityError
from flask import (
    Blueprint, g, flash, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash
from budget_app.init_db import get_db

# bp = Blueprint('auth', __name__, url_prefix='/auth')
bp = Blueprint('auth', __name__)

# ========================= Helper functions ================================ #

@bp.before_app_request
def load_logged_in_user():
    user_id = session.get('user_id')
    if user_id is None:
        g.user = None
    else:
        cursor = get_db().cursor()
        cursor.execute(
            'SELECT * FROM users where id = (%s)', (user_id,)
        )
        g.user = cursor.fetchone()

def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for('auth.login_user'))
        return view(**kwargs)
    return wrapped_view

# ============================== Routes ===================================== #

@bp.route('/register', methods=('GET', 'POST'))
def register_user():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        first_name = request.form['firstName']
        last_name = request.form['lastName']
        db = get_db()
        error = None
        if not username:
            error = 'Username is required'
        if not password:
            error = 'Password is required'
        if error is None:
            try:
                db.cursor().execute(
                    'INSERT INTO users (username, first_name, last_name, password) VALUES (%s, %s, %s, %s)', (username, first_name, last_name, generate_password_hash(password))
                )
                db.commit()
            except IntegrityError as e:
                error = f'user {username} is already registered'
            else:
                return redirect(url_for('auth.login_user'))
        flash(error)
    return render_template('/auth/register.html')

@bp.route('/login', methods=('GET', 'POST'))
def login_user():
    if request.method == 'POST':
    # get db/make db connection and check username and password
        db = get_db()
        username = request.form['username']
        password = request.form['password']
        error = None
        if not username:
            # error = 'Username is required to login'
            flash('Username is required', 'error_username')
        if not password:
            # error = 'Password is required to login'
            flash('Password is required', 'error_password')
        if error is None:
            try:
                cursor = db.cursor()
                cursor.execute(
                    # 'INSERT INTO users (username, first_name, last_name, password) VALUES (%s, %s, %s, %s)', (username, first_name, last_name, password)
                    'SELECT * FROM users WHERE username = (%s)', (username,)
                )
                user = cursor.fetchall()
                if check_password_hash(user[0][4], password):
                    session.clear()
                    session['user_id'] = user[0][0]
                    session['username'] = user[0][1]
                    return redirect(url_for('transactions.home'))
                else:
                    error = f'Passwords do not match'
            # if username's password != retrieved password
            except IndexError as e:
                error = f'{username} does not exist'
        flash(error)
    return render_template('/auth/login.html', session=session)

@bp.route('/logout')
def logout_user():
    session.clear()
    return redirect(url_for('auth.login_user'))