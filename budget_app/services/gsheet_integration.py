from budget_app.data_pipeline.load.sheets_handler import GoogleHandler


def run_sheets_update(processsed_data, username):
    google_handler = GoogleHandler(processsed_data, username)
    google_handler.run_sheets_update()