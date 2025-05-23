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
import calendar
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

    def run_cleaner_and_return_data(self):
        self.convert_direntrys_to_dfs()
        self.normalize_data()

    def convert_direntrys_to_dfs(self):
        for direntry in self.input_banking_data:
            if "csv" in direntry.name.lower():
                banking_data_df = pd.read_csv(direntry.path)
            elif "xlsx" in direntry.name.lower():
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
            # Use dynamic slice objects to do this!
            # 
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
            # loop through terms to remove
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOMATIC PAYMENT -')]
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOPAY PAYMENT -')]
        normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('Bill Pay Payment')]
        # Create unique record id
            # move to method
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
            # BILT has no headers: date amount star blank description
            if dataframe.iloc[0,0].strip() == 'Prepared for':
                # Get column headers so they can be normalized
                dataframe.columns = [col for col in dataframe.iloc[5]]
                dataframe.drop(dataframe.index[0:6], inplace=True)
                dataframe.reset_index(inplace=True)
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

    def data_filter_by_date(self, data_to_filter):
        filter = (data_to_filter.loc[:, 'Transaction Date'] >= f'{self.todays_date.year}-{self.todays_date.month}-01') & (data_to_filter.loc[:, 'Transaction Date'] <= f'{self.todays_date.year}-{self.todays_date.month}-{self.number_of_days_in_current_month}')
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

def get_normalized_data_frame(raw_banking_files):
    data_cleaner = DataCleaner(raw_banking_files)
    data_cleaner.run_cleaner_and_return_data()
    return data_cleaner.normalized_data_frame