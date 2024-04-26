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

import json

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'
SAMPLE_RANGE_title = 'August-AZ!A:D'


class GoogleHandler(object):

    data_range = 'test'
    user = 'AZ'
    
    def __init__(self, date_ranges, banking_data='PLACEHOLDER', user='AZ'):
        self.user = user
        self.date_ranges = date_ranges
        self.banking_data = banking_data
        self.spreadsheet_id = '1Nvt_5JvU1Bkrhn2hofFiFPTtJblOsoRW5q2kbBxn168'
        self.today_date = date.today()
        self.creds = self.get_creds_and_build()[0]
        self.service = self.get_creds_and_build()[1]

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

    def check_for_sheet(self):
        # Call the Sheets API
        # create access to the google sheets "services" -> get spreadsheet 
        service = build('sheets', 'v4', credentials=self.creds)
        working_sheet = service.spreadsheets()
        working_sheet_values = working_sheet.values()

        # ========================== Check for tabs ========================== #
        # First step is to check to see if a tab for the given month/user exists
            # To check for an existing tab, we need a way to read the tabs and compare against what we expect the tab for the given data should look like
            # Tab name should take the form "date_user".  This date/use will come as a parameter when instantiating this class

        # Get metadata
        sheet_metadata = working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        # Get all sheet/tab names
        allsheets = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]

        for date_range, data_frame_by_month in self.banking_data.items():
            date_range_to_add = f'{date_range}-{self.user}'
            if date_range_to_add in allsheets:
                # UPDATE SHEET
                response = self.update_sheet(date_range_to_add, data_frame_by_month)
            # If sheet isn't in all the sheets, create a new sheet and add data  => Create new sheet: month_user 
            else:
                # CREATE SHEET
                # UPDATE SHEET
                new_sheet_creation_response = self.create_new_sheet(working_sheet, date_range_to_add)
                new_sheet_added_title = new_sheet_creation_response['replies'][0]['addSheet']['properties']['title']
                new_sheet_added_sheetId = new_sheet_creation_response['replies'][0]['addSheet']['properties']['sheetId']
                
                self.new_sheet_added_sheetId = new_sheet_added_sheetId
                self.copy_and_paste_budget_to_new_sheet(new_sheet_added_sheetId)
                response = self.update_sheet(new_sheet_added_title, data_frame_by_month)
        # get sheet id so you can format the new sheet
        for sheet in working_sheet.get(spreadsheetId=SPREADSHEET_ID).execute()['sheets']:
            if re.search('[0-9]{4}-[0-9]+', sheet['properties']['title']):
                # self.format_sheet()
                # print(sheet['properties']['sheetId'])
                self.format_sheet(sheet['properties']['sheetId'])

            
    def create_new_sheet(self, working_sheet, date_range):
        body = {
            "requests" : [
                {
                    "addSheet" : {
                        "properties" : {
                            "title" : date_range,
                            "tabColor": {
                                'red': 1.2,
                                'green': 1.3,
                                'blue': 1.4
                            }
                        }
                    },
                }
            ]
        }
        sheet_added_response = working_sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()        
        return sheet_added_response

    def update_sheet(self, sheet_title, data_frame):
        # use the "build"/connection to the specific "service" api and "resource" = build(serviceName, 'V4', creds=self.creds).spreadsheets.values()
        working_sheet = self.service.spreadsheets().values()
        # loop through a list of lists of data
        body = {
            'valueInputOption':'USER_ENTERED',
            'data': [
                {
                    # this will eventually come from the return of the method before this
                    # Sheet1!A:Z -> spreadsheet info comes from the batchUpdate() arguments 
                    "range": f'{sheet_title}!F:Z',
                    "majorDimension": 'ROWS',
                    # values to be added
                    # "values": self.banking_data
                    "values": data_frame
                },
            ]
        }
        # after the sheet has been updated, format it
        return working_sheet.batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()

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
                        # "startIndex": 0,
                        # "endIndex": 9
                    },
                    "properties": {
                        "pixelSize": 18
                    },
                    "fields": "pixelSize"
                    }
                }
            ]
                # "requests": [
                # {
                #     "updateDimensionProperties": {
                #     "range": {
                #         "sheetId": sheet_id,
                #         "dimension": "COLUMNS",
                #         "startIndex": 5,
                #         "endIndex": 9
                #     },
                #     "properties": {
                #         "pixelSize": 260
                #     },
                #     "fields": "pixelSize"
                #     }
                # }
                # ]
        }
        response = working_sheet.batchUpdate(spreadsheetId = self.spreadsheet_id, body=body).execute()
        return response