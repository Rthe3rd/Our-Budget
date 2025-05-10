from budget_app.data_pipeline.load.data_in_out_handler import get_files
from budget_app.data_pipeline.transform.data_cleaner import get_normalized_data_frame
from budget_app.services.db_operations import DBHandler
from budget_app.services.file_processing import process_transactions
from budget_app.services.gsheet_integration import run_sheets_update
from budget_app.data_pipeline.load.sheets_handler import GoogleHandler 

def run_transaction_upload(username):
    # get the raw banking files that are in the data_in folder, normalize and return them as a singluar DataFrame
    raw_banking_files = get_files()
    if not raw_banking_files:
        return False

    # Process the uploaded files
    transactions_from_upload = get_normalized_data_frame(raw_banking_files)

    # Pandas stuff
    processed_data = process_transactions(username, transactions_from_upload)
    
    if not processed_data.get('transactions_to_add_to_google_sheets').empty:
        run_sheets_update(processed_data, username)

        # This can be tucked away into the class module?
        # google_handler = GoogleHandler(date_ranges=processed_data['date_ranges'], banking_data=processed_data['list_of_dataframes_by_month'], user=username)
        # google_handler.run_sheets_update()

    # insert all new records into the db
    if processed_data.get('transactions_to_add_to_db').any():
        DBHandler.insert_transactions_into_db(username, processed_data['transactions_to_add_to_db'])

    return True
