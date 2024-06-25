# **************************************************************************************************** #
# Test file to explore handling Chase statements - Read me?
# Eventually expand to handle all finacial statements and connect to budgeting app

# **************************************************************************************************** #
# GOALS
    # MINI-TERM: Creates a nicely formated file to copy and pasted
    # MVP-TERM: Update google sheets

# **************************************************************************************************** #
# ACTIONS
    # File intake
    # Format data: Transaction - Date - Description	- Category - Amount
        # Expenditures come in as negative values
        # Ignore certain, regular, transactions: Payments
        # Unique entries: Since data may be refreshed multiple times a month, records should not be duplicated
            # Possible solutions: each record is given a synthetic category that is constant and checked against
    # Update google sheets
        # Need to research: https://developers.google.com/sheets/api/quickstart/python
        # Google Workspace API
        # Prerequisites:
            # Python 3.10.7 or greater
            # The pip package management tool
            # A Google Cloud project.
            # A Google Account.

# **************************************************************************************************** #
import csv
import datetime
import calendar
# import openpyxl
import numpy as np
import pandas as pd
from datetime import date
import re


class DataCleaner(object):

    def __init__(self, input_banking_data):
        self.columns_to_drop = ['Post Date', 'Memo', 'Type', 'Extended Details', 'Appears On Your Statement As','Address', 'City/State' ,'Zip Code', 'Country', 'Reference']
        self.todays_date = date.today()
        self.number_of_days_in_current_month = calendar.monthrange(date.today().year, date.today().month)[1]
        self.input_banking_data = input_banking_data
        self.dfs_to_process = []
        self.clean_banking_data_frames = []
        # previously was a list; see package_data_frames()
        self.data_to_pass_to_google_handler = ''
        self.normalized_data_frame = ''

    def clean_data(self):
        self.convert_direntrys_to_dfs()
        self.normalize_columns_headers()
        # self.normalize_columns_and_size()
        self.normalize_data()
        # self.package_data_frames()

    def run_cleaner_and_return_data(self):
        self.convert_direntrys_to_dfs()
        self.normalize_data()

    def convert_direntrys_to_dfs(self):
        for direntry in self.input_banking_data:
            if "csv" in direntry.name.lower():
                banking_data_df = pd.read_csv(direntry.path)
            elif "xlsx" in direntry.name.lower():
                # AMEX needs to target 'Sheet1' -> excel_file.sheet_names
                banking_data_df = pd.ExcelFile(direntry.path)
                if 'Transaction Details' in banking_data_df.sheet_names:
                    banking_data_df = pd.read_excel(direntry.path, sheet_name='Transaction Details', engine='openpyxl')
                else:
                    banking_data_df = pd.read_excel(direntry.path, engine='openpyxl')
            self.dfs_to_process.append(banking_data_df)

    def normalize_data(self):
        normalized_data_frames = []
        if not isinstance(self.dfs_to_process, list):
            self.dfs_to_process = [self.dfs_to_process]
        for data_frame in self.dfs_to_process:
            data_fame_columns = [str(column).lower().strip() for column in data_frame.columns.to_list()]  
            normalized_data_frame = pd.DataFrame()
            if 'american' in "".join(data_fame_columns):
                normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0] 
                normalized_data_frame['description'] = data_frame.iloc[:, 1] 
                normalized_data_frame['category'] = data_frame.iloc[:, 10] 
                normalized_data_frame['amount'] = data_frame.iloc[:, 2] 
                normalized_data_frame.drop(data_frame.index[0:6], inplace=True)
            elif 'post date' in data_fame_columns:
                normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0]
                normalized_data_frame['description'] = data_frame.iloc[:, 2]
                normalized_data_frame['category'] = data_frame.iloc[:, 3]
                normalized_data_frame['amount'] = data_frame.iloc[:, 5]
            elif '*' in data_fame_columns:
                normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0]
                normalized_data_frame['description'] = data_frame.iloc[:, 4]
                normalized_data_frame['category'] = data_frame.iloc[:, 2]
                normalized_data_frame['amount'] = data_frame.iloc[:, 1]
            normalized_data_frames.append(normalized_data_frame)
        normalized_data_frame = pd.concat(normalized_data_frames, axis=0)
        normalized_data_frame['transaction_date'] = pd.to_datetime(normalized_data_frame['transaction_date'])
        normalized_data_frame['amount'] = normalized_data_frame['amount'].abs().astype(float)
        # Removing payments - Chase, Amex, Bilt
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOMATIC PAYMENT -')]
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOPAY PAYMENT -')]
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('Bill Pay Payment')]
        # Create Unique record
        transaction_date = normalized_data_frame['transaction_date'].apply(lambda x: x.strftime("%Y-%m-%d").replace("-","")[2:]).astype(str)
        description = normalized_data_frame['description'].apply(lambda x:  re.compile('[\W_]+').sub('', x))
        amount = normalized_data_frame['amount'].apply(lambda x: str(x).replace(".",""))
        normalized_data_frame['unique_record_id'] = (transaction_date + amount + description).apply(lambda x:  x[:20] if len(x) >= 20 else  x + '0' * (20-len(x)))    
        self.normalized_data_frame = normalized_data_frame
        return normalized_data_frame


    def data_filter_by_date_temp(self):
        transaction_date = self.normalized_and_sized_data_frame['Transaction Date'].apply(lambda x: x.strftime("%Y-%m-%d").replace("-","")[2:]).astype(str)
        description = self.normalized_and_sized_data_frame['Description'].apply(lambda x:  re.compile('[\W_]+').sub('', x))
        amount = self.normalized_and_sized_data_frame['Amount'].apply(lambda x: str(x).replace(".",""))
        self.normalized_and_sized_data_frame['Unique Record Id'] = (transaction_date + amount + description).apply(lambda x:  x[:15] if len(x) >= 15 else  x + '0' * (15-len(x)))   

    def normalize_columns_headers(self):
        # This is where the type of statement, from which bank the statement orginates from. start at bottom left/right?
        data_frames_holder = []
        for dataframe in self.dfs_to_process:
            # Remember: Amex has funky headers - give all dfs' the correct header?
            # BILT has no headers: date amount star blank description
            if dataframe.iloc[0,0].strip() == 'Prepared for':
                # Get column headers so they can be normalized
                dataframe.columns = [col for col in dataframe.iloc[5]]
                dataframe.drop(dataframe.index[0:6], inplace=True)
                dataframe.reset_index(inplace=True)
            # elif isinstance(dataframe.iloc[0,0], pd.Timestamp):
                # dimensions don't equate, need to fix!
            elif dataframe.iloc[0,0].split('/') != 0:
                column_headers = ['Transaction Date', 'Description', 'Category', 'Amount']
                df_headers = pd.DataFrame(column_headers, columns=dataframe.columns)
                dataframe = pd.concat([df_headers, dataframe], ignore_index=True)
            for col_name in dataframe.columns:
                if col_name.strip() in self.columns_to_drop:
                    dataframe.drop([col_name], axis=1, inplace=True)
            dataframe = dataframe.rename(columns={'Date': 'Transaction Date', })
            dataframe = dataframe.loc[:, ['Transaction Date', 'Description', 'Category', 'Amount']]
            dataframe['Unique Record Id'] = pd.Series(dtype=str)
            data_frames_holder.append(dataframe)
        self.dfs_to_process = data_frames_holder

    # def normalize_data(self):
    #     self.dfs_to_process = pd.concat(self.dfs_to_process, ignore_index=True)
    #     self.dfs_to_process['Transaction Date'] = pd.to_datetime(self.dfs_to_process['Transaction Date'])
    #     # How should this step be removed?
    #     self.dfs_to_process = pd.DataFrame(self.dfs_to_process[self.data_filter_by_date(self.dfs_to_process)])
    #     self.dfs_to_process['Amount'] = self.dfs_to_process['Amount'].abs().astype(float)
    #     self.dfs_to_process = self.dfs_to_process.loc[self.dfs_to_process['Description'] != 'Automatic Payment - Thank']
    #     self.dfs_to_process = self.dfs_to_process.loc[self.dfs_to_process['Description'] != 'AUTOPAY PAYMENT - THANK YOU']
    #     self.dfs_to_process['Description'] = self.dfs_to_process.loc[:, 'Description'].apply(lambda x : x.title())

    def data_filter_by_date(self, data_to_filter):
        filter = (data_to_filter.loc[:, 'Transaction Date'] >= f'{self.todays_date.year}-{self.todays_date.month}-01') & (data_to_filter.loc[:, 'Transaction Date'] <= f'{self.todays_date.year}-{self.todays_date.month}-{self.number_of_days_in_current_month}')
        # df produces a series of True/False values, as does "filter" 
        # -> data_to_filter.loc[(data_to_filter['Transaction Date'] >= '2023-11-01') & (data_to_filter['Transaction Date'] <= '2023-11-30')] -> is cleaner
        # df = (data_to_filter['Transaction Date'] >= '2023-11-01') & (data_to_filter['Transaction Date'] <= '2023-11-30')
        return pd.DataFrame(filter.values, columns=['Transaction Date']).iloc[:,0]

    def create_unique_record_id(self):
        transaction_date = self.dfs_to_process['Transaction Date'].apply(lambda x: x.strftime("%Y-%m-%d").replace("-","")[2:]).astype(str)
        description = self.dfs_to_process['Description'].apply(lambda x:  re.compile('[\W_]+').sub('', x))
        amount = self.dfs_to_process['Amount'].apply(lambda x: str(x).replace(".",""))
        self.dfs_to_process['Unique Record Id'] = (transaction_date + amount + description).apply(lambda x:  x[:15] if len(x) >= 15 else  x + '0' * (15-len(x)))        

    def package_data_frames(self):
        dfs_to_process = pd.concat(self.dfs_to_process, ignore_index=True)
        data_frames = dfs_to_process.map(str)
        data_to_pass_to_google_handler = [data_frames.columns.values.tolist()] 
        data_to_pass_to_google_handler.extend(data_frames.values.tolist())
        self.data_to_pass_to_google_handler = dfs_to_process













    # Under construction method/to replace/generalize current normalize_columns_headers() method 
    def normalize_columns_headers_temp(self):
        self.date_column_indices = self.find_date_column()
        self.description_column_indices = self.find_description_column()
        self.amount_column_indices = self.find_amount_column()
        self.category_column_indices = self.find_category_column()

    # function returns the integer value of the column or "iloc" of a "date looking" value
    def find_date_column(self):
        date_pattern_dash = '([0-9]+-[0-9]{1,2}-[0-9]+)' 
        date_pattern_slash = '([0-9]+/[0-9]{1,2}/[0-9]+)'
        date_column_indices = []
        if not isinstance(self.dfs_to_process, list):
            self.dfs_to_process = [self.dfs_to_process]
        for data_frame in self.dfs_to_process:
            i = 0
            for value in data_frame.iloc[-1]:
                value_str = str(value)
                if re.search(date_pattern_dash, value_str) or re.search(date_pattern_slash, value_str):
                    date_column_indices.append(i)
                    break
                else: 
                    i += 1
        return date_column_indices

    def find_description_column(self):
        description_column_indices = []
        if not isinstance(self.dfs_to_process, list):
            self.dfs_to_process = [self.dfs_to_process]
        for df in self.dfs_to_process:
            if 'american' in "".join([str(column).lower().strip() for column in df.columns.to_list()]):
                description_column_indices.append(1)
            elif 'Description' in df.columns.to_list():
                description_column_indices.append(df.columns.to_list().index('Description'))
            elif '*' in df.columns.tolist():
                description_column_indices.append(4)
        return description_column_indices
        
    def find_amount_column(self):
        amount_column_indices = []
        if not isinstance(self.dfs_to_process, list):
            self.dfs_to_process = [self.dfs_to_process]
        for data_frame in self.dfs_to_process:
            columns = [str(column).lower().replace('-','').replace('.', '') for column in data_frame.columns.tolist()]
            numeric_columns = ['True' if column.isnumeric() else 'False' for column in columns]
            if 'american' in "".join([str(column).lower().strip() for column in data_frame.columns.to_list()]):
                amount_column_indices.append(2)
            elif 'amount' in columns:
                amount_column_indices.append(columns.index('amount')) 
            elif 'True' in numeric_columns:
                amount_column_indices.append(numeric_columns.index('True'))
        return amount_column_indices

    def find_category_column(self):
        category_column_indices = []
        if not isinstance(self.dfs_to_process, list):
            self.dfs_to_process = [self.dfs_to_process]
        for data_frame in self.dfs_to_process:
            columns = [str(column).lower().replace('-','').replace('.', '') for column in data_frame.columns.tolist()]
            if 'category' in columns:
                category_column_indices.append(columns.index('category'))
            elif 'american' in "".join([str(column).lower().strip() for column in data_frame.columns.to_list()]):
                category_column_indices.append(10)
            else:
                category_column_indices.append(3)
        return category_column_indices