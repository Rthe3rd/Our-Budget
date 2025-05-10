# pandas stuff returns: transactions_to_add_to_google_sheets, transactions_to_add_to_db, list_of_dataframes_by_month, date_ranges
from collections import defaultdict
import pandas as pd
from budget_app.services.db_operations import DBHandler


def process_transactions(username, transactions_from_upload):
    pulled_transactions = pd.Series(transaction[0] for transaction in DBHandler.get_pulled_transactions(username, transactions_from_upload))

    # remove any records that are already in the db from the transactions to add to the db
    if pulled_transactions.empty:
        transactions_to_add_to_db = transactions_from_upload.values
        transactions_to_add_to_google_sheets = transactions_from_upload
    else:
        transactions_to_add_to_db = transactions_from_upload[~transactions_from_upload['unique_record_id'].isin(pulled_transactions)].values.tolist()
        transactions_to_add_to_google_sheets = transactions_from_upload[~transactions_from_upload['unique_record_id'].isin(pulled_transactions)]

    # This seams like a lot of specific, pre-work to work "nicely" within the handler
        # Something more generic should be here in place and then the handler decides how it wants to work with the data provided
    date_ranges = pd.to_datetime(transactions_to_add_to_google_sheets['transaction_date']).dt.to_period('M').drop_duplicates()

    date_ranges = [f'{date_range.year}' + "-" + f'{date_range.month}' for date_range in date_ranges]
    date_ranges.reverse()

    processed_data = defaultdict(None, {
        'transactions_to_add_to_google_sheets': transactions_to_add_to_google_sheets,
        'transactions_to_add_to_db': transactions_to_add_to_db, 
        'date_ranges': date_ranges
        })

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
        processed_data['list_of_dataframes_by_month'] = list_of_dataframes_by_month

    return processed_data