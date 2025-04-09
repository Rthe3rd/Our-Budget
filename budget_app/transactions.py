# authorized users can submit their expenses to the database
# import authorization login required
# import db and perform actions
import os
from flask import (
    Flask, Blueprint, flash, g, redirect, render_template, request, url_for, current_app, session
)
from werkzeug.utils import secure_filename
from werkzeug.exceptions import abort
from budget_app.auth import login_required
from budget_app.data_pipeline.transform.transactions import run_transaction_upload

# ========================= Temporary Imports ================================ #
from budget_app.data_pipeline.load.data_in_out_handler import remove_specfied_file

bp = Blueprint('transactions', __name__)

# ========================= Helper functions ================================ #

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']

# ============================== Routes ===================================== #

@bp.route('/', methods=('GET', 'POST'))
@login_required
def home():
    files = [file for file in os.listdir(os.path.join(os.getcwd(), 'budget_app/data_pipeline/data_in'))]
    paths = [os.path.join('budget_app/data_pipeline/data_in', file) for file in os.listdir('budget_app/data_pipeline/data_in')]
    files_and_paths = dict(zip(files, paths))
    return render_template('/transactions/transactions.html', files=files, files_and_paths=files_and_paths)

# submit a transaction
@bp.route('/submit_transaction', methods=('GET', 'POST'))
@login_required
def submit_transaction():
    # save expenses to db
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('File required!')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('transactions.home', name=filename))
    return redirect(url_for('transactions.home'))

@bp.route('/upload_transactions')
def upload_transactions():

    if not run_transaction_upload(session.get('username')):
        flash('No transactions submitted', category='no_uploads')
        return redirect(url_for('transactions.home')) 

    return redirect(url_for('transactions.home'))


@bp.route('/remove_statement', methods=('GET', 'POST'))
def remove_statement():
    if request.method == 'GET':
        remove_specfied_file(request.args.get('path'))
    return redirect(url_for('transactions.home'))