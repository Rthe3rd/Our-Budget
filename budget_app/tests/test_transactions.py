import os
from flask import (
    Flask, Blueprint, flash, g, redirect, render_template, request, url_for, current_app, session
)

import re
import pandas as pd
from budget_app.data_pipeline.transform.data_cleaner import DataCleaner
from budget_app.data_pipeline.load.data_in_out_handler import get_files
from budget_app.data_pipeline.load.sheets_handler import GoogleHandler
from budget_app.init_db import get_db


test_df = pd.DataFrame({
    'Transaction Date': pd.to_datetime(['2023-01-01']),
    'Description': ['Example Description'],
    'Category': ['Generic Category'],  
    'Amount': pd.to_numeric([44.55]),
    'Unique Record Id': ['ABC12345defg']
    })

files = [file for file in os.scandir(os.path.join(os.getcwd(), 'budget_app/tests/test_data'))]
data_cleaner = DataCleaner(files)

def test_transaction():
    raw_banking_files = get_files()
    data_cleaner = DataCleaner(raw_banking_files)
    data_cleaner.clean_data()
    all_transactions = data_cleaner.data_to_pass_to_google_handler

    # get connections, get cursor, perform execution on cursors and commit
    db = get_db()
    cursor = db.cursor()
    username = session.get('username')
    cursor.execute(
        'SELECT id FROM users WHERE username = %s', (username,)
    )
    user_id = cursor.fetchone()[0]

    # Currently extracting by the current month, how should it be?  
        # pull all unique records, regardless of date range, for username
    # cursor.execute(
    #     'SELECT unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s AND transaction_date <= %s', (user_id, current_app.config['FIRST_OF_MONTH'], current_app.config['LAST_OF_MONTH'])
    # )
    cursor.execute(
        'SELECT unique_record_id FROM transactions WHERE user_id = %s', (user_id)
    )


    pulled_transactions = cursor.fetchall()
    pulled_transactions = pd.Series(transaction[0] for transaction in pulled_transactions)
    transactions_to_add_to_db = all_transactions[~all_transactions['Unique Record Id'].isin(pulled_transactions)].values.tolist()
    transactions_to_add_to_sheets = all_transactions[~all_transactions['Unique Record Id'].isin(pulled_transactions)]

    for transaction in transactions_to_add_to_db:
        cursor.execute(
            'INSERT INTO transactions VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, DEFAULT);', (user_id, *transaction,)
        )
        db.commit()

    cursor.execute(
        "SELECT to_char(transaction_date, 'YYYY-MM-DD'), description, category, amount, unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s AND transaction_date <= %s", (user_id, current_app.config['FIRST_OF_MONTH'], current_app.config['LAST_OF_MONTH']) 
    )
    records_to_add_to_sheets = cursor.fetchall()

    columns_to_add_to_sheet = [transactions_to_add_to_sheets.map(str).columns.tolist()]
    records_to_add_to_sheets = records_to_add_to_sheets
    
    transactions_to_add_to_sheets = [*columns_to_add_to_sheet, *records_to_add_to_sheets]
    google_handler = GoogleHandler(transactions_to_add_to_sheets, username)
    google_handler.check_for_sheet()
    # move_files_out(current_app.config['UPLOAD_FOLDER'], current_app.config['OUTPUT_FOLDER'])