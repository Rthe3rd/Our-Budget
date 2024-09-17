import os
from flask import (
    Flask, Blueprint, flash, g, redirect, render_template, request, url_for, current_app, session
)

import re
import pandas as pd
from budget_app.data_pipeline.transform.data_cleaner import DataCleaner
from budget_app.data_pipeline.load.data_in_out_handler import get_files
from budget_app.data_pipeline.load.sheets_handler import GoogleHandler
from budget_app.init_db import get_db


test_df = pd.DataFrame({
    # 'Transaction Date': [pd.to_datetime(['2023-01-01']), pd.to_datetime(['2023-02-10']), pd.to_datetime(['2023-02-18'])],
    'Transaction Date': ['2023-01-01', '2023-02-10', '2023-02-18'],
    'Description': ['Example Description 1', 'Example Description 2', 'Example Description 3'],
    'Category': ['Generic Category 1', 'Generic Category 2', 'Generic Category 3'],  
    'Amount': [pd.to_numeric([44.55]), pd.to_numeric([692.30]), pd.to_numeric([10.75])],
    'Unique Record Id': ['ABC12345defg', 'GHASD234KK', 'UBNasdf1234']
    })

files = [file for file in os.scandir(os.path.join(os.getcwd(), 'budget_app/tests/test_data'))]
data_cleaner = DataCleaner(files)

def test_get_date_ranges_from_uploaded_transactions():
    date_ranges = pd.to_datetime(test_df['Transaction Date']).dt.to_period('M').drop_duplicates()
    for month in date_ranges:
        assert '' == ''

def test_build_sheets_name_to_update(username='alex_zuniga'):
    date_ranges = pd.to_datetime(test_df['Transaction Date']).dt.to_period('M').drop_duplicates()
    for date_range in date_ranges:
        assert f"{date_range.year}-{date_range.month}-{username}" == ''
