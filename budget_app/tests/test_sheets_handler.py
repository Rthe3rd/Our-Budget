from budget_app.data_pipeline.load.sheets_handler import GoogleHandler
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource



import os.path
import re
from datetime import date

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
SPREADSHEET_ID = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'

# def test_creds():
#     google_Handler = GoogleHandler()
#     assert isinstance(google_Handler.get_creds_and_build()[0], Credentials)

# def test_service():
#     google_Handler = GoogleHandler()
#     assert isinstance(google_Handler.get_creds_and_build()[1], Resource)
class GoogleHandler(object):

    data_range = 'test'
    user = 'AZ'
    
    def __init__(self, date_ranges=None, banking_data='PLACEHOLDER', user='alex'):
        self.user = user
        self.date_ranges = date_ranges
        self.banking_data = banking_data
        self.spreadsheet_id = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'
        self.creds = self.get_creds_and_build()[0]
        self.service = self.get_creds_and_build()[1]
    
    def build_sheet(self):
            # Call the Sheets API
            # create access to the google sheets "services" -> get spreadsheet 
            service = build('sheets', 'v4', credentials=self.creds)
            working_sheet = service.spreadsheets()
            return working_sheet

    def get_creds_and_build(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        try:
            service = build('sheets', 'v4', credentials=creds)

        except HttpError as err:
            print(err)
        return [creds, service]

'''
The goal is to extract a sheetId from the spreadsheets/tabs that have a matching month
The months will be held in an array and the month will noted as their integer value: jan = 1, feb = 2 etc.
example_sheet_metadata = {
        'sheets':{
            'properties':{
                'sheetId': 0,
                'title': '2024-1-example',
                'index': 0,
                'sheetType': 'GRID',
                'GridProperties': 
                {'rowCount':100, 'columnCount':100},
                'hidden': True
            },
            'merges':{
                ['array of start/end row/column indices of merged cells']
            }
        },
        'other fields':{
            'other nested fields': 'other data'
        }
    }
'''
# requires user_name => should this be passed by a class or arguement => thinking the former
def test_find_sheets_given_month_user(self: str = 'alex', months_to_update: list[int] = []) -> list[str]:
    google_handler = GoogleHandler()
    working_sheet = google_handler.build_sheet()
    
    sheets_metadata = working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheets_metadata['sheets']
    # months_to_update comes from transactions.py
    months_to_update = [5,6]
    sheets_to_update = []
    for month in months_to_update:
        # username 
        if [sheet['properties'] for sheet in sheets if re.match(f'[0-9]+-{month}-{self}', sheet['properties']['title'])]:
            sheet_to_update_metadata = [sheet['properties'] for sheet in sheets if re.match(f'[0-9]+-{month}-{self}', sheet['properties']['title'])][0]
            sheets_to_update.append(sheet_to_update_metadata)
    # return list of all sheets objects
    assert len(sheets_to_update) == 2

test_sheets = [{
    'sheetId': 599577025, 
    'title': '2024-5-alex_zuniga-proto', 
    'index': 35,
    'sheetType': 'GRID',
    'gridProperties': {
        'rowCount': 1000, 
        'columnCount': 26
        }, 
    'tabColor': {
        'red': 0.19607843,
        'green': 0.29411766,
        'blue': 0.39607844
        }, 
    'tabColorStyle': {
        'rgbColor': {
            'red': 0.19607843,
            'green': 0.29411766,
            'blue': 0.39607844
            }
        }
    }, {
    'sheetId': 1876948372, 
    'title': '2024-6-alex_zuniga', 
    'index': 37, 
    'sheetType': 'GRID',
    'gridProperties': {
        'rowCount': 1000, 
        'columnCount': 26
        }, 
        'tabColor': {
            'red': 0.49019608, 
            'green': 0.29803923, 
            'blue': 0.49019608
        }, 
        'tabColorStyle': {
            'rgbColor': {
                'red': 0.49019608, 
                'green': 0.29803923, 
                'blue': 0.49019608
                }
            }
        }
    ]

def test_find_update_start(self=None, sheets_to_update=test_sheets):
    google_handler = GoogleHandler()
    working_sheet = google_handler.build_sheet()
    column_letter = 'F'
    for sheet in sheets_to_update:
        values_in_column = working_sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range = f"'{sheet['title']}'!{column_letter}:{column_letter}"
        ).execute()
        sheet['values_in_column'] = values_in_column.get('values', [])
    assert len(sheets_to_update[0]['values_in_column']) == 44

def test_collect_uploaded_unique_records(self=None, sheets_to_update=test_sheets):
    google_handler = GoogleHandler()
    working_sheet = google_handler.service.spreadsheets()
    unique_records = working_sheet.values().batchGet(
        spreadsheetId=SPREADSHEET_ID,
        ranges="2024-5-alex_zuniga-proto!J:J"
    ).execute()
    unique_records_hash = set([record[0] for record in unique_records['valueRanges'][0]['values']])
    assert unique_records_hash == ''
