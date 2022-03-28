import os
import glob
import pandas as pd
import yaml
from datetime import date
from datetime import datetime
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

    # Create dictionary for all files
    all_dfs = {}

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

        # Create LA file dictionary
        LA_dfs = {}
        
        # Find 903 files in LA folder
        s903_files = glob.glob(os.path.join(input_folder, la, "*.csv"))
        print("{} --- Found {} SSDA903 files".format(la, len(s903_files)))

        # Read csvs into dict of dataframes
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

                # Degrade sensitive fields:
                #   - DOB to MOB
                #   - Postcodes to postcode sectors

                if file_type != "Episodes":
                    try:
                        clean_df["DOB"] = clean_df["DOB"].dt.to_period('M').dt.to_timestamp()
                    except:
                        pass

                if file_type == "Header":
                    try:
                        clean_df["MC_DOB"] = clean_df["MC_DOB"].dt.to_period('M').dt.to_timestamp()
                    except:
                        pass

                if file_type == "Episodes":
                    clean_df["HOME_POST"] = clean_df["HOME_POST"].str.strip()
                    clean_df["HOME_POST"] = clean_df["HOME_POST"].str[:-2]

                    clean_df["PL_POST"] = clean_df["PL_POST"].str.strip()
                    clean_df["PL_POST"] = clean_df["PL_POST"].str[:-2]

                # Add year and la as columns to the df
                clean_df["Year"] = year
                clean_df["LA"] = la

                # Add the df to the dfs dict if less than four years old
                # Find current year and month
                current_year = date.today().year
                current_month = date.today().month
                
                # Determine financial year of latest return
                if current_month <=4:
                    latest_return = current_year - 1
                else: latest_return = current_year
                
                # if return is less than four years old:
                # adds df to la_dfs dict within list corresponding to file type
                if latest_return - int(year) <= 3:
                    if file_type in LA_dfs.keys():
                        LA_dfs[file_type].append(clean_df)
                    else:
                        LA_dfs[file_type] = []
                        LA_dfs[file_type].append(clean_df)

            # If file is not a match provide file details to user
            else:
                print(
                    "Failed to match {}: {} to known column names".format(file_name, list(loaded_file.columns)))

        # Concatenate multiple years of dfs for the same LA by file type
        # De-duplicates Episodes files based on specified fields
        # Output csv of concatenated files in LA folder
        # Add concatenated file to all_files dictionary
        p = os.path.join(Inputs, la, "Outputs")
        for key in LA_dfs.keys():
            df_name = la + "_" + key
            concatenated_file = pd.concat(LA_dfs[key])
            if key == "Episodes":
                concatenated_file = concatenated_file.sort_values('Year', ascending = False)
                concatenated_file = concatenated_file.drop_duplicates(subset=['CHILD', 'DECOM', 'LS', 'PLACE', 'PLACE_PROVIDER', 'PL_POST'], keep = 'first')
            concatenated_file.to_csv(os.path.join(p, "{}_cleaned.csv".format(df_name)))
            if key in all_dfs.keys():
                all_dfs[key].append(concatenated_file)
            else:
                all_dfs[key] = []
                all_dfs[key].append(concatenated_file)

    # Concatenate multiple las' dfs by file type
    # Output csv of concatenated files in Outputs folder
    for key in all_dfs.keys():
        concatenated_file = pd.concat(all_dfs[key])
        concatenated_file.to_csv(os.path.join(Outputs, "{}_cleaned.csv".format(key)))

    return

def match_load_file(df):
    '''Matches the loaded csv against one of the 10 903 file types'''
    for table_name, expected_columns in column_names.items():
        if set(df.columns) == set(expected_columns):
            print("Loaded {} from csv".format(table_name))
            return table_name

def cleandf(df, config):
    '''Checks the types of each column:
        - changes floats to integers
        - for dates, passes column through date format function
        - for categories, passes column through category format function
        - for others (strings and integers), passes through other format function'''
    for column in df.columns:
        if df[column].dtype == "float64":
            df[column] = df[column].astype("Int64")
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
    errors = 0
    # Tries to detect strings as correct date format;
    # when it cannot:
    # deletes entry (to minimise risk of disclosing sensitive information)
    # and returns error
    bool_series = pd.notnull(series)
    for row in series[bool_series]:
        try:
            datetime.strptime(row, config['date'])
        except:
            series = series.replace(row,"")
            errors += 1
    # Counts errors and reports back
    if errors > 0:
        print("{} - {} entries could not be formatted as dates".format(name, errors))
    # Counts blank entries where there shouldn't be any
    if config['canbeblank'] is False:
        blanks = series.isnull().sum()
    # Outputs non-zero blank counts for user
    if blanks > 0:
        print("{} - {} blank entries found".format(name, blanks))
    # Configures series to dates, having deleting non-conforming entries
    try:
        series = pd.to_datetime(series)
    except:
        pass
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