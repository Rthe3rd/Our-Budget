import re
import os
import pandas as pd
import numpy as np
from budget_app.data_pipeline.transform.data_cleaner import DataCleaner

test_df = pd.DataFrame({
    'Transaction Date': pd.to_datetime(['2023-01-01']),
    'Description': ['Example Description'],
    'Category': ['Generic Category'],  
    'Amount': pd.to_numeric([44.55]),
    'Unique Record Id': ['ABC12345defg']
    })

files = [file for file in os.scandir(os.path.join(os.getcwd(), 'budget_app/tests/test_data'))]
data_cleaner = DataCleaner(files)
# data_cleaner.convert_direntrys_to_dfs()
# data_cleaner.normalize_columns_headers()
# data_cleaner.normalize_data()

def test_convert_direntrys_dfs():
    data_cleaner.convert_direntrys_to_dfs()
    dfs = data_cleaner.dfs_to_process
    for df in dfs:
        assert isinstance(df, pd.DataFrame)

# def test_normalize_columns_headers():
#     data_cleaner.normalize_columns_headers()
#     dfs = data_cleaner.dfs_to_process
    # for df in dfs:
    #     for i in range(len(df.columns)):
    #         assert df.columns[i] == test_df.columns[i]

# def test_normalize_data():
#     data_cleaner.normalize_data()
#     dfs = data_cleaner.dfs_to_process
#     for column_name in dfs.columns:
#         check_data_type(dfs, column_name, test_df[column_name].dtype)

# def test_create_unique_record_id():
#     data_cleaner.create_unique_record_id()
#     for value in data_cleaner.dfs_to_process['Unique Record Id'].values:
#         assert len(value) == 15

def check_data_type(data_frame, column_name, expected_data_type):
    assert data_frame[column_name].dtype == expected_data_type

def test_data_type():
    date_pattern_dash = '([0-9]+-[0-9]{1,2}-[0-9]+)' 
    date_pattern_slash = '([0-9]+/[0-9]{1,2}/[0-9]+)'
    if not isinstance(data_cleaner.dfs_to_process, list):
        data_cleaner.dfs_to_process = [data_cleaner.dfs_to_process]
    for data_frame in data_cleaner.dfs_to_process:
        for index, row in data_frame.iterrows():
            for value in row.tolist():
                value_str = str(value)
                # if re.search(date_pattern_dash, value_str) or re.search(date_pattern_slash, value_str):
                #     print(row.tolist().index(value))

def test_find_date_column():
    date_pattern_dash = '([0-9]+-[0-9]{1,2}-[0-9]+)' 
    date_pattern_slash = '([0-9]+/[0-9]{1,2}/[0-9]+)'
    date_column_indices = []
    if not isinstance(data_cleaner.dfs_to_process, list):
        data_cleaner.dfs_to_process = [data_cleaner.dfs_to_process]
    for data_frame in data_cleaner.dfs_to_process:
        i = 0
        # for index, row in data_frame.iterrows():
        for value in data_frame.iloc[-1]:
            value_str = str(value)
            if re.search(date_pattern_dash, value_str) or re.search(date_pattern_slash, value_str):
                date_column_indices.append(i)
                break
            else: 
                i += 1
    assert date_column_indices == [0, 0, 0, 0]

def test_find_description_column():
    description_column_indices = []
    for df in data_cleaner.dfs_to_process:
        if 'american' in "".join([str(column).lower().strip() for column in df.columns.to_list()]):
            description_column_indices.append(1)
        elif 'Description' in df.columns.to_list():
            description_column_indices.append(df.columns.to_list().index('Description'))
        elif '*' in df.columns.tolist():
            description_column_indices.append(4)
    assert description_column_indices == [1, 4, 2, 4]
    
def test_find_amount_column():
    amount_column_indices = []
    for data_frame in data_cleaner.dfs_to_process:
        columns = [str(column).lower().replace('-','').replace('.', '') for column in data_frame.columns.tolist()]
        numeric_columns = ['True' if column.isnumeric() else 'False' for column in columns]
        if 'american' in "".join([str(column).lower().strip() for column in data_frame.columns.to_list()]):
            amount_column_indices.append(2)
        elif 'amount' in columns:
            amount_column_indices.append(columns.index('amount')) 
        elif 'True' in numeric_columns:
            amount_column_indices.append(numeric_columns.index('True'))
    assert amount_column_indices == [2, 1, 5, 1]

def test_find_category_column():
    category_column_indices = []
    for data_frame in data_cleaner.dfs_to_process:
        columns = [str(column).lower().replace('-','').replace('.', '') for column in data_frame.columns.tolist()]
        if 'category' in columns:
            category_column_indices.append(columns.index('category'))
        elif 'american' in "".join([str(column).lower().strip() for column in data_frame.columns.to_list()]):
            category_column_indices.append(10)
        else:
            category_column_indices.append(3)
    assert category_column_indices == [10, 3, 3, 3]

def test_column_headers():

    # for column in test_df.column.totist():
    # for i in range(len(dfs.columns.totist())):
        # print(test_df.columns.to_list())
    all_data = pd.DataFrame()
    for column_header in ['Transaction Date','Description', 'Category', 'Amount']:
        # all_data = pd.DataFrame['Transaction Date']
        date_column_indices = [0, 0, 0, 0]
        description_column_indices = [1, 4, 2, 4]
        amount_column_indices = [2, 1, 5, 1]
        category_column_indices = [10, 3, 3, 3]
        for i in range(len(data_cleaner.dfs_to_process)):
            all_data['Transaction Date'] = data_cleaner.dfs_to_process[i].iloc[:,date_column_indices[i]]
            all_data['Description'] = data_cleaner.dfs_to_process[i].iloc[:,description_column_indices[i]]
            all_data['Category'] = data_cleaner.dfs_to_process[i].iloc[:,category_column_indices[i]]
            all_data['Amount'] = data_cleaner.dfs_to_process[i].iloc[:,amount_column_indices[i]]
    # assert all_data.columns.to_list() == ['Transaction Date','Description', 'Category', 'Amount']
    # print(all_data['Amount'])
    # assert all_data['Amount'] == test_df['Amount']
            
def test_clean_irregular_data_frames():
    normalized_data_frames = []
    if not isinstance(data_cleaner.dfs_to_process, list):
        data_cleaner.dfs_to_process = [data_cleaner.dfs_to_process]
    for data_frame in data_cleaner.dfs_to_process:
        data_fame_columns = [str(column).lower().strip() for column in data_frame.columns.to_list()]  
        normalized_data_frame = pd.DataFrame()
        if 'american' in "".join(data_fame_columns):
            normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0] 
            normalized_data_frame['description'] = data_frame.iloc[:, 1] 
            normalized_data_frame['category'] = data_frame.iloc[:, 10] 
            normalized_data_frame['amount'] = data_frame.iloc[:, 2] 
            normalized_data_frame.drop(data_frame.index[0:6], inplace=True)
            normalized_data_frame['transaction_date'] = pd.to_datetime(normalized_data_frame['transaction_date'])
        elif 'post date' in data_fame_columns:
            normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0]
            normalized_data_frame['description'] = data_frame.iloc[:, 2]
            normalized_data_frame['category'] = data_frame.iloc[:, 3]
            normalized_data_frame['amount'] = data_frame.iloc[:, 5]
            normalized_data_frame['transaction_date'] = pd.to_datetime(normalized_data_frame['transaction_date'])
        elif '*' in data_fame_columns:
            normalized_data_frame['transaction_date'] = data_frame.iloc[:, 0]
            normalized_data_frame['description'] = data_frame.iloc[:, 4]
            normalized_data_frame['category'] = data_frame.iloc[:, 2]
            normalized_data_frame['amount'] = data_frame.iloc[:, 1]
            normalized_data_frame['transaction_date'] = pd.to_datetime(normalized_data_frame['transaction_date'])
        normalized_data_frames.append(normalized_data_frame)
    normalized_data_frame = pd.concat(normalized_data_frames, axis=0)
    normalized_data_frame['amount'] = normalized_data_frame['amount'].abs().astype(float)
    # Removing payments - Chase, Amex, Bilt
    normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOMATIC PAYMENT -')]
    normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('AUTOPAY PAYMENT -')]
    normalized_data_frame = normalized_data_frame[~normalized_data_frame['description'].str.contains('Bill Pay Payment')]
    # create unique record ID
    transaction_date = normalized_data_frame['transaction_date'].apply(lambda x: x.strftime("%Y-%m-%d").replace("-","")[2:]).astype(str)
    description = normalized_data_frame['description'].apply(lambda x:  re.compile('[\W_]+').sub('', x))
    amount = normalized_data_frame['amount'].apply(lambda x: str(x).replace(".",""))
    normalized_data_frame['unique_record_id'] = (transaction_date + amount + description).apply(lambda x:  x[:20] if len(x) >= 20 else  x + '0' * (20-len(x)))
    
