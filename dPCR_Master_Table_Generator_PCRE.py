import pandas as pd
import os
import numpy as np
from datetime import date

today = date.today()
d4 = today.strftime("%d-%b-%Y")
print (d4)
path = os.getcwd()
# Go up one level
parent_dir = os.path.dirname(path)

# Find the 'dPCR' directory
dPCR_dir = os.path.join(parent_dir, 'QIAcuity_30PCRE05')
dPCR_dir2 = os.path.join(parent_dir, 'QIAcuity_30PCRE06')


def process_folder(folder_path):
    
    paths = []
    numOfFile = 0
    today = date.today()
    d4 = today.strftime("%d-%b-%Y")
    df = pd.DataFrame()
    # Check if the folder exists
    if os.path.exists(folder_path):    
        for root, dirs, files in os.walk(folder_path):  # Use folder_path instead of path
            for file in files:
                if '~$' in file:
                    continue
                elif file.__contains__("Worksheet") or file.__contains__("Quantitation") and file[-4:] == 'xlsx':
                    #print(os.path.join(root, file))
                    s = os.path.join(root, file)
                    #print(s)
                    paths.append(s)
                    numOfFile = numOfFile + 1
                    #print (numOfFile)

    print (numOfFile,'files found and merging')

    for f in paths:
        predata = pd.ExcelFile(f)
        
        if "Results" in predata.sheet_names:
            data = pd.read_excel(f, "Results", skiprows=17, nrows=32, usecols='A:I')
            data2 = pd.read_excel(f, skiprows=4, nrows=100, usecols='A:Q')
            data3 = pd.read_excel(f, nrows=1, usecols='Q')
            data4 = pd.read_excel(f, nrows=1, usecols='B')
            data5 = pd.read_excel(f, nrows=1, usecols='G')
            if not data2.empty:
                data2['date'] = data3.columns[0]
                data2['experiment ID'] = data4.columns[0]
                data2['Run ID'] = data5.columns[0]
                # Split the file path into its components
                path_parts = os.path.normpath(f).split(os.sep)

                # Assign the folderName and fileName to the data DataFrame
                data['folderName'] = path_parts[-2]
                data['fileName'] = path_parts[-1]
                if 'Sample/NTC/Control' in data2.columns:
                    merdata = pd.merge(data, data2, left_on = 'Sample ID', right_on='Sample/NTC/Control', how='left')
                    #df = df.append(merdata)
                    df = pd.concat([df, merdata], axis=0)

    df = df.dropna(subset=['Sample/NTC/Control'])
    df = df.dropna(axis=1, how="all")

    column_mapping = {
        'Sample Description_x': 'Sample Description_y',
        'Sample Description_y': 'Sample Description',
        'NT-175 KICN': 'NT-175ID CN'
        }

    for old_col, new_col in column_mapping.items():
        if old_col in df.columns and new_col in df.columns:
            df[new_col] = df[old_col].combine_first(df[new_col])

    # Assuming df is your original dataframe and 'col1', 'col2', 'col3' are the columns to consider for duplicate removal
    new_df = df.drop_duplicates(subset=['Sample Description', 'Sample ID', 'experiment ID'])
    new_df = new_df[['Sample Description', 'Sample ID', 'experiment ID']]
 

    # Get the current working directory
    working_dir = os.path.dirname(os.path.realpath(__file__))
    # Define the paths to the Excel files
    excel_file_path1 = os.path.join(working_dir, f'{os.path.basename(folder_path)}_Master_Table_{d4}.xlsx')
    excel_file_path2 = os.path.join(working_dir, f'{os.path.basename(folder_path)}_Join_Table_{d4}.xlsx')
    # Export the dataframes to Excel
    df.to_excel(excel_file_path1, index=False)
    new_df.to_excel(excel_file_path2, index=False)
     
# Process the 'dPCR_dir' folder
process_folder(dPCR_dir)

# Process the 'dPCR_dir2' folder
process_folder(dPCR_dir2)

# Append the two dataframes and merge with join table
df = pd.DataFrame()
df2 = pd.DataFrame()
#edit the date
file_name = f'{os.path.basename(dPCR_dir)}_Master_Table_{d4}.xlsx'
file_name2 = f'{os.path.basename(dPCR_dir2)}_Master_Table_{d4}.xlsx'
file_name3 = f'{os.path.basename(dPCR_dir)}_Join_Table_{d4}.xlsx'
file_name4 = f'{os.path.basename(dPCR_dir2)}_Join_Table_{d4}.xlsx'
file_name5 = "Old_Join_Table.xlsx"
df = pd.read_excel(file_name)
df2 = pd.read_excel(file_name2)
df3 = pd.read_excel(file_name3)
df4 = pd.read_excel(file_name4)
df5 = pd.read_excel(file_name5)

df5 = df5.drop_duplicates(subset=['Sample ID', 'Sample Description', 'experiment ID'])

combined_df = df.append(df2, ignore_index=True)
combined_join_df = df3.append(df4, ignore_index=True)
combined_join_df = pd.merge(combined_join_df, df5, on=['Sample ID','Sample Description','experiment ID'], how='left')

writer = pd.ExcelWriter('Combined_Master_Table.xlsx')
writer2 = pd.ExcelWriter('Combined_Join_Table.xlsx')
combined_df.to_excel(writer)
combined_join_df.to_excel(writer2)

writer.save()
writer2.save()