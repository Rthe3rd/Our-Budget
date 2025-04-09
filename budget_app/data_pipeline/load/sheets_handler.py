# This file is to handle the connection with Google Sheets API
# ==================== API KEY ==================== #
# key=

from __future__ import print_function

import os.path
import re
from datetime import date

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pandas import DataFrame

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'
SAMPLE_RANGE_title = 'August-AZ!A:D'


class GoogleHandler(object):
    def __init__(self, date_ranges: list[list], banking_data, user=str):
        self.user = user
        self.date_ranges = date_ranges
        self.banking_data = banking_data
        self.spreadsheet_id = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'
        self.creds = self.get_creds_and_build()[0]
        self.service = self.get_creds_and_build()[1]

    def get_creds_and_build(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is created automatically when the authorization flow completes for the first time.
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

    # ========================== Check for tabs ========================== #
    # First step is to check to see if a tab for the given month/user exists
        # To check for an existing tab, we need a way to read the tabs and compare against what we expect the tab for the given data should look like
        # Tab name should take the form "date_user".  This date/use will come as a parameter when instantiating this class
    def run_sheets_update(self):
        # Call the Sheets API
        working_sheet = self.service.spreadsheets()
        # Get metadata
        sheet_metadata = working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        # Get all sheet/tab names
        allsheets = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]
        for date_range, data_frame_by_month in self.banking_data.items():

            date_range_to_add = f'{date_range}-{self.user}'

            # If sheet isn't in all the sheets, create a new sheet and add data  => Create new sheet: month_user 
            if not date_range_to_add in allsheets:
                new_sheet_creation_response = self.create_new_sheet(working_sheet, date_range_to_add)
                date_range_to_add = new_sheet_creation_response['replies'][0]['addSheet']['properties']['title']
                new_sheet_added_sheetId = new_sheet_creation_response['replies'][0]['addSheet']['properties']['sheetId']
                self.new_sheet_added_sheetId = new_sheet_added_sheetId
                self.copy_and_paste_budget_to_new_sheet(new_sheet_added_sheetId)

            # UPDATE SHEET
            response = self.update_sheet(date_range_to_add, data_frame_by_month)


        # get sheet id so you can format the new sheet
        for sheet in working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()['sheets']:
            if re.search('[0-9]{4}-[0-9]+', sheet['properties']['title']):
                self.format_sheet(sheet['properties']['sheetId'])

    # ========================== Create new sheet ========================== #
    # Request body to generate a new tab/sheet given a date range and worksheet/working_sheet
    def create_new_sheet(self, working_sheet, date_range: str) -> None:
        body = {
            "requests" : [
                {
                    "addSheet" : {
                        "properties" : {
                            "title" : date_range,
                            "tabColor": {
                                'red': 0.5,
                                'green': 1.5,
                                'blue': 2.5
                            }
                        }
                    },
                }
            ]
        }
        sheet_added_response = working_sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()        
        return sheet_added_response

    # ========================== Update sheet ========================== #
    # Send the data to update a month tab/sheet

    def update_sheet(self, sheet_title: str, data_frame: DataFrame) -> None:
        # use the "build"/connection to the specific "service" api and "resource" = build(serviceName, 'V4', creds=self.creds).spreadsheets.values()
        working_sheet = self.service.spreadsheets().values()
        month = sheet_title.split('-')[1]

        # Use the given month to find the sheet/tab to update
        sheet_to_update = self.find_sheets_given_month_user([month])[0]
        # Use the sheet/tab to update to find where the new data starts or the "range to update"
        sheet_to_update_with_range = self.find_update_start([sheet_to_update])[0]
        range_to_update = f'{sheet_to_update_with_range["title"]}!F{len(sheet_to_update_with_range["values_in_column"]) + 1}:Z'

        print(data_frame)
        # loop through a list of lists of data
        body = {
            'valueInputOption':'USER_ENTERED',
            'data': [
                {
                    # Sheet1!A:Z -> spreadsheet info comes from the batchUpdate() arguments 
                    "range": f'{range_to_update}',
                    "majorDimension": 'ROWS',
                    "values": data_frame[1:]
                },
            ]
        }
        return working_sheet.batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()

    def find_sheets_given_month_user(self, months_to_update: list[int] = []) -> list[str]:
        working_sheet = self.service.spreadsheets()
        sheets_metadata = working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheets_metadata['sheets']
        sheets_to_update = []

        for month in months_to_update:
            if [sheet['properties'] for sheet in sheets if re.match(f'[0-9]+-{month}-{self.user}', sheet['properties']['title'])]:
                sheet_to_update_metadata = [sheet['properties'] for sheet in sheets if re.match(f'[0-9]+-{month}-{self.user}', sheet['properties']['title'])][0]
                sheets_to_update.append(sheet_to_update_metadata)
        return sheets_to_update

    def find_update_start(self, sheets_to_update: list[object]) -> list[object]:
        working_sheet = self.service.spreadsheets()
        column_letter = 'F'
        for sheet in sheets_to_update:
            values_in_column = working_sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range = f"'{sheet['title']}'!{column_letter}:{column_letter}"
            ).execute()
            sheet['values_in_column'] = values_in_column.get('values', [])
        return sheets_to_update

    def copy_and_paste_budget_to_new_sheet(self, sheetId):
        working_sheet = self.service.spreadsheets()
        body = {
            'requests': [
                {
                    'copyPaste': {
                        'source': {
                            'sheetId': '239343090',
                            'startRowIndex': 0,
                            'endRowIndex': 34,
                            'startColumnIndex': 0,
                            'endColumnIndex': 3,
                        },
                        'destination': {
                            'sheetId': sheetId,
                            'startRowIndex': 0,
                            'endRowIndex': 34,
                            'startColumnIndex': 0,
                            'endColumnIndex': 3,
                        },
                        'pasteType': 'PASTE_NORMAL',
                        'pasteOrientation': 'NORMAL'
                    }
                }
            ]
        }
        response = working_sheet.batchUpdate(spreadsheetId = self.spreadsheet_id, body=body).execute()
        return response

    def format_sheet(self, sheet_id):
        working_sheet = self.service.spreadsheets()
        body = {
            'requests': [
                {
                    'autoResizeDimensions': {
                        'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 10
                        }
                    }
                },
                {
                    "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                    },
                    "properties": {
                        "pixelSize": 18
                    },
                    "fields": "pixelSize"
                    }
                }
            ]
        }
        response = working_sheet.batchUpdate(spreadsheetId = self.spreadsheet_id, body=body).execute()
        return response