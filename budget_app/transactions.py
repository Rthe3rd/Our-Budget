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
from budget_app.init_db import get_db

# ========================= Temporary Imports ================================ #
from budget_app.data_pipeline.load.data_in_out_handler import get_files, move_files_out, remove_specfied_file
from budget_app.data_pipeline.transform.data_cleaner import DataCleaner
from budget_app.data_pipeline.load.sheets_handler import GoogleHandler
import pandas as pd


bp = Blueprint('transactions', __name__)

# ========================= Helper functions ================================ #

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']

# ============================== Routes ===================================== #

@bp.route('/home', methods=('GET', 'POST'))
@login_required
def home():
    files = [file for file in os.listdir(os.path.join(os.getcwd(), 'budget_app/data_pipeline/data_in'))]
    paths = [os.path.join('budget_app/data_pipeline/data_in', file) for file in os.listdir('budget_app/data_pipeline/data_in')]
    files_and_paths = dict(zip(files, paths))
    return render_template('/transactions/transactions.html', files=files, files_and_paths=files_and_paths)

@bp.route('/submit_transaction', methods=('GET', 'POST'))
@login_required
def submit_transaction():
    # save expenses to db
    # should this trigger an updating of the google sheet all in one
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
    # get the raw banking files that are in the data_in folder, normalize and return them as a singluar DataFram
    raw_banking_files = get_files()
    data_cleaner = DataCleaner(raw_banking_files)
    data_cleaner.run_cleaner_and_return_data()
    transactions_from_upload = data_cleaner.normalized_data_frame

    # get connections, get cursor, perform execution on cursors and commit
    db = get_db()
    cursor = db.cursor()
    username = session.get('username')
    cursor.execute(
        'SELECT id FROM users WHERE username = %s', (username,)
    )
    user_id = cursor.fetchone()[0]
    cursor.execute(
        'SELECT unique_record_id FROM transactions WHERE user_id = %s', (user_id,)
    )

    pulled_transactions = cursor.fetchall()
    pulled_transactions = pd.Series(transaction[0] for transaction in pulled_transactions)


    # remove any records that are already in the db from the transactions to add to the db
    if pulled_transactions.empty:
        transactions_to_add_to_db = transactions_from_upload.values
    else:
        transactions_to_add_to_db = transactions_from_upload[~transactions_from_upload['unique_record_id'].isin(pulled_transactions)].values.tolist()

    # insert all new records into the db
    for transaction in transactions_to_add_to_db:
        cursor.execute(
            'INSERT INTO transactions VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, DEFAULT);', (user_id, *transaction,)
        )
        db.commit()

    # grab all records from all the months spanned by the dates in the transactions uploaded 
    from_date = transactions_from_upload['transaction_date'].min()
    cursor.execute(
        "SELECT to_char(transaction_date, 'YYYY-MM-DD'), description, category, amount, unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s", (user_id, from_date) 
    )
    records_to_add_to_sheets = cursor.fetchall()

    # 
    date_ranges = transactions_from_upload['transaction_date'].dt.to_period('M').drop_duplicates()
    # cursor.execute(
    #     "SELECT to_char(transaction_date, 'YYYY-MM-DD'), description, category, amount, unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s AND transaction_date <= %s", (user_id, current_app.config['FIRST_OF_MONTH'], current_app.config['LAST_OF_MONTH']) 
    # )
    # this can be an environment variable?
    columns = ['Transaction Date','Description', 'Category', 'Amount', 'Unique Record Id']
    # df = pd.DataFrame(list_of_tuples, columns=['Name', 'Age', 'Occupation'])

    # convert records to add to the sheets to a data frame for manipulation
    records_to_add_to_sheets = pd.DataFrame(records_to_add_to_sheets, columns=columns)
    records_to_add_to_sheets['Transaction Date'] = pd.to_datetime(records_to_add_to_sheets['Transaction Date'])
    
    # Group DataFrame by year and month
    # grouped = df.groupby([df['date'].dt.year, df['date'].dt.month]
    grouped = records_to_add_to_sheets.groupby([records_to_add_to_sheets['Transaction Date'].dt.year, records_to_add_to_sheets['Transaction Date'].dt.month])
    # Create a list of DataFrames, each containing records from the same month and year
    list_of_dataframes_by_month = [group_df.reset_index(drop=True) for _, group_df in grouped]

    flattened_data_frames = []
    for data_frame in list_of_dataframes_by_month:
        data_frame['Transaction Date'] = data_frame['Transaction Date'].dt.strftime('%m/%d/%Y')
        data_frame.columns = columns
        # [data_frame.columns.values.tolist()].extend(data_frame.values.tolist())
        flattened_data_frames.append([columns, *data_frame.values.tolist()])
        # print(type(data_frame.values.tolist()))

    # data_to_pass_to_google_handler = [data_frames.columns.values.tolist()] 
    # data_to_pass_to_google_handler.extend(data_frames.values.tolist())

    # month: data_frame_by_month
    date_ranges = [f'{date_range.year}' + "-" + f'{date_range.month}' for date_range in date_ranges]
    date_ranges.reverse()
    # list_of_dataframes_by_month = dict(zip(date_ranges, list_of_dataframes_by_month))
    list_of_dataframes_by_month = dict(zip(date_ranges, flattened_data_frames))

    # STARTING THE GOOGLE SHEETS HANDLER PROCESS
    # pass the listified, stringified, dictionary of lists of lists to the GoogleeHandler
    # google_handler = GoogleHandler(date_ranges=date_ranges, banking_data=list_of_dataframes_by_month, user=username )
    google_handler = GoogleHandler(date_ranges=date_ranges, banking_data=list_of_dataframes_by_month, user=username )
    google_handler.check_for_sheet()

    # columns_to_add_to_sheet = [transactions_to_add_to_sheets.map(str).columns.tolist()]
    # records_to_add_to_sheets = records_to_add_to_sheets
    
    # transactions_to_add_to_sheets = [*columns_to_add_to_sheet, *records_to_add_to_sheets]
    # google_handler = GoogleHandler(transactions_to_add_to_sheets, username)
    # google_handler.check_for_sheet()
    # move_files_out(current_app.config['UPLOAD_FOLDER'], current_app.config['OUTPUT_FOLDER'])

    return redirect(url_for('transactions.home'))


@bp.route('/remove_statement', methods=('GET', 'POST'))
def remove_statement():
    if request.method == 'GET':
        remove_specfied_file(request.args.get('path'))
    return redirect(url_for('transactions.home'))