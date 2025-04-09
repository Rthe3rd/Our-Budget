import pandas as pd
from budget_app.data_pipeline.load.data_in_out_handler import get_files
from budget_app.data_pipeline.transform.data_cleaner import DataCleaner
from budget_app.data_pipeline.load.sheets_handler import GoogleHandler
from budget_app.init_db import get_db


def run_transaction_upload(username):
    # get the raw banking files that are in the data_in folder, normalize and return them as a singluar DataFrame
    raw_banking_files = get_files()
    if not raw_banking_files:
        return False

    data_cleaner = DataCleaner(raw_banking_files)
    data_cleaner.run_cleaner_and_return_data()
    transactions_from_upload = data_cleaner.normalized_data_frame

    # convert pulled transactions to a pandas series for better handling
    pulled_transactions = pd.Series(transaction[0] for transaction in get_pulled_transactions(username, transactions_from_upload))

    # remove any records that are already in the db from the transactions to add to the db
    if pulled_transactions.empty:
        transactions_to_add_to_db = transactions_from_upload.values
        transactions_to_add_to_google_sheets = transactions_from_upload
    else:
        transactions_to_add_to_db = transactions_from_upload[~transactions_from_upload['unique_record_id'].isin(pulled_transactions)].values.tolist()
        transactions_to_add_to_google_sheets = transactions_from_upload[~transactions_from_upload['unique_record_id'].isin(pulled_transactions)]

    date_ranges = pd.to_datetime(transactions_to_add_to_google_sheets['transaction_date']).dt.to_period('M').drop_duplicates()

    date_ranges = [f'{date_range.year}' + "-" + f'{date_range.month}' for date_range in date_ranges]
    date_ranges.reverse()

    if len(transactions_to_add_to_google_sheets['unique_record_id'].values):
        columns = ['Transaction Date','Description', 'Category', 'Amount', 'Unique Record Id']
        transactions_to_add_to_google_sheets.columns = columns
        transactions_to_add_to_google_sheets['Transaction Date'] = pd.to_datetime(transactions_to_add_to_google_sheets['Transaction Date'])

        # Group DataFrame by year and month
        grouped = transactions_to_add_to_google_sheets.groupby([transactions_to_add_to_google_sheets['Transaction Date'].dt.year, transactions_to_add_to_google_sheets['Transaction Date'].dt.month])
        # Create a list of DataFrames, each containing records from the same month and year
        list_of_dataframes_by_month = [group_df.reset_index(drop=True) for _, group_df in grouped]

        flattened_data_frames = []
        for data_frame in list_of_dataframes_by_month:
            data_frame['Transaction Date'] = data_frame['Transaction Date'].dt.strftime('%m/%d/%Y')
            data_frame.columns = columns
            flattened_data_frames.append([columns, *data_frame.values.tolist()])
        list_of_dataframes_by_month = dict(zip(date_ranges, flattened_data_frames))

        google_handler = GoogleHandler(date_ranges=date_ranges, banking_data=list_of_dataframes_by_month, user=username)
        # google_handler.run_sheets_update()

    # insert all new records into the db
    insert_transactions_into_db(username, transactions_to_add_to_db)

    return True

def get_user_id(cursor, username):
    cursor.execute(
        'SELECT id FROM users WHERE username = %s', (username,)
    )
    return cursor.fetchone()[0]

def get_db_cursor():
    db = get_db()
    return db, db.cursor()

def insert_transactions_into_db(username, transactions_to_add_to_db):
    db, cursor = get_db_cursor()
    user_id = get_user_id(cursor, username)

    for transaction in transactions_to_add_to_db:
        cursor.execute(
            'INSERT INTO transactions VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, DEFAULT);', (user_id, *transaction,)
        )

def get_pulled_transactions(username, transactions_from_upload):
    # get user_id from db that matches the current user_id in session
    db, cursor = get_db_cursor()
    user_id = get_user_id(cursor, username)

    # get all records with user id and after the earliest date
    from_date = transactions_from_upload['transaction_date'].min()
    cursor.execute(
        'SELECT unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s', (user_id, from_date)
    )
    db.commit()

    return cursor.fetchall()  