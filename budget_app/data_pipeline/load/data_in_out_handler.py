import os
import shutil
from flask import current_app

def get_files():
    # previously was getting an object with a name/path
    if os.scandir(os.path.join(os.getcwd(), 'budget_app/data_pipeline/data_in')):
        files = [file for file in os.scandir(os.path.join(os.getcwd(), 'budget_app/data_pipeline/data_in'))]
    return files

def move_files_out():
    file_paths = [file.path for file in os.scandir('budget_app/data_pipeline/data_in') if file.name != '.DS_Store']
    file_names = [file.name for file in os.scandir('budget_app/data_pipeline/data_in') if file.name != '.DS_Store']
    for file_path in file_paths:
        shutil.move(file_path, 'budget_app/tests/test_data')

def move_files_out(source_folder, destination_folder):
    for filename in os.listdir(source_folder):
        source_path = os.path.join(source_folder, filename)
        destination_path = os.path.join(destination_folder, filename)
        
        # Check if the file already exists in the destination folder
        if os.path.exists(destination_path):
            # Append a number to the filename to avoid overwriting
            base, extension = os.path.splitext(filename)
            count = 1
            while True:
                new_filename = f"{base}_{count}{extension}"
                new_destination_path = os.path.join(destination_folder, new_filename)
                if not os.path.exists(new_destination_path):
                    break
                count += 1
            destination_path = new_destination_path
        
        # Move the file to the destination folder
        shutil.move(source_path, destination_path)
        print(f"Moved {source_path} to {destination_path}")

def remove_specfied_file(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
    return f'File from {filepath} had been remmoved'