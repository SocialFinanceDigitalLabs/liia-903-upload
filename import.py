import os
import glob
import pandas as pd
import yaml
from datetime import date
from columns import column_names

main_folder = os.getcwd()
Inputs = os.path.join(main_folder, 'Inputs')
Outputs = os.path.join(main_folder, 'Outputs')

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

def main(input_folder, output_folder, config, process_missing_only=True):
    '''Identifies all the LAs in the input folder
    - Option to only process files that aren't already processed in output folder
    - Matches files to SSDA903 module
    - Concatenates up to four years' worth of 903 csvs for a single LA and de-duplicates them
    - Concatenates multiple LAs' 903s into single csvs
    - Outputs the merged/cleaned csvs to LA and LIIA folders'''

    # Identify LAs
    # All
    all_las = os.listdir(input_folder)
    # Already processed
    processed_las = [x.split('_')[0] for x in os.listdir(output_folder)]

    # Process all or only missing data
    if process_missing_only:
        las_to_process = [la for la in all_las if la not in processed_las]
    else:
        las_to_process = all_las

    # Process each LA that needs to be processed
    print("Processing {} LAs: {}".format(len(las_to_process), las_to_process))
    for la in las_to_process:

        # Find 903 files in LA folder
        s903_files = glob.glob(os.path.join(input_folder, la, "*.csv"))
        print("{} --- Found {} SSDA903 files".format(la, len(s903_files)))

        # Read csvs into dict of dataframes
        s903_dfs = {}
        for i, file in enumerate(s903_files):
            print("File {} out of {}".format(i+1, len(s903_files)))
            print("--- Load file {}".format(i+1))

            # Extract the year of the return
            # Search for year, not a specific character placement
            file_name = os.path.basename(file)
            year = file_name[8:12]

            # Load the file as a DataFrame
            loaded_file = pd.read_csv(file)

            # Match the df to the 903 module
            file_type = match_load_file(loaded_file)

            # Continue with cleaning and loading operations if file is matched
            if file_type is not None:

                # Check the columns are in the correct format
                clean_df = cleandf(loaded_file, config[file_type])

                # Add year and la as columns to the df
                clean_df[year] = year
                clean_df[la] = la

                # Create module_year name for df
                df_name = file_type + year

                # Add the df to the dfs dict if less than four years old
                # Find current year and month
                current_year = date.today().year
                current_month = date.today().month
                
                # Determine financial year of latest return
                if current_month <=4:
                    latest_return = current_year - 1
                else: latest_return = current_year
                
                # Add file to dfs dict if return is less than four years old
                if latest_return - int(year) <= 3:
                    s903_dfs[df_name] = clean_df

            # If file is not a match provide file details to user
            else:
                print(
                    "Failed to match {}: {} to known column names".format(file_name, list(loaded_file.columns)))

        # Test: print keys of dict
        print(s903_dfs.keys())

            # PLACEHOLDER Save in output folder
    return

def match_load_file(df):
    '''Matches the loaded csv against one of the 10 903 file types'''
    for table_name, expected_columns in column_names.items():
        if set(df.columns) == set(expected_columns):
            print("Loaded {} from csv".format(table_name))
            return table_name

def cleandf(df, config):
    '''Checks the types of each column:
        - for dates, passes column through date format function
        - for categories, passes column through category format function
        - for others (strings and integers), passes through other format function'''
    for column in df.columns:
        column_type = list(config[column].keys())[0]
        if column_type == "date":
            df[column] = date_check(df[column], config[column], column)
        elif column_type == 'category':
            df[column] = cat_check(df[column], config[column], column)
        else:
            other_check(df[column], config[column], column)
    return df

def date_check(series, config, name):
    '''Checks date fields for two sets of inconsistencies with the specified configuration:
       - for all fields, it applies a standard date format
       - where it can't apply this format, it returns an error for the field
       - where a field should always have an entry, it provides a count of blank rows to the user'''
    blanks = 0
    # Tries to format non-blank entries; returns error if not possible
    try:
        series = pd.to_datetime(series)
    except:
        print("Error - {} cannot be parsed as a date".format(name))
    # Counts blank entries where there shouldn't be any
    if config['canbeblank'] is False:
        blanks = series.isnull().sum()
    # Outputs non-zero blank counts for user
    if blanks > 0:
        print("{} - {} blank entries found".format(name, blanks))
    return series

def cat_check(series, config, name):
    '''Checks category fields for two sets of inconsistencies with the specified configuration:
       - for all fields, it matches entries against the config for all non-blank rows
       - where it can't find a match, it replaces the string with a format error
       - a count of errors is provided to the user
       - where a field should always have an entry, it provides a count of blank rows to the user'''
    errors = 0
    codename = 0
    blanks = 0
    # Tries to match non-blank entries against config
    bool_series = pd.notnull(series)
    for row in series[bool_series]:
        match = 0
        for code in config['category']:
            if str(row).lower() == str(code['code']).lower():
                match += 1
            elif 'name' in code:
                if str(code['name']).lower() in str(row).lower():
                    codename += 1
                    match += 1
        if match == 0:
            errors += 1
# If names have been detected, replaces names with codes
    if codename > 0:
        series.replace(config['dictionary'], inplace=True)
    # or counts blank entries where there shouldn't be any
    if config['canbeblank'] is False:
        blanks = series.isnull().sum()
    # Outputs non-zero error / blank counts for user
    if errors > 0:
        print("{} - {} invalid entries".format(name, errors))
    if blanks > 0:
        print("{} - {} blank entries found".format(name, blanks))
    return series

def other_check(series, config, name):
    '''Checks string and number fields for whether there are blank entries when there shouldn't be'''
    blanks = 0
    if config['canbeblank'] is False:
        blanks = series.isnull().sum()
    if blanks > 0:
        print("{} - {} blank entries found".format(name, blanks))
    return

main(Inputs, Outputs, config)