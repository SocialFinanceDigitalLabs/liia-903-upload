import pandas as pd
import yaml
from pathlib import Path

loaded_file = pd.read_csv(r"C:\Users\patrick.troy\OneDrive - Social Finance Ltd\Work\Python\liia 903 upload\LDS\\"
                          r"LA1\903\Inputs\SSDA903_2020_ad1.csv")


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
            print(df[column])
        #     df[column] = date_check(df[column], config[column], column)
        # elif column_type == 'category':
        #     df[column] = cat_check(df[column], config[column], column)
        # else:
        #     other_check(df[column], config[column], column)
    return df


def load_config():
    """
    Find and return the config yaml file
    """
    data_dir = Path(r"C:\Users\patrick.troy\OneDrive - Social Finance Ltd\Work\Python\liia 903 upload")
    for yaml_file in data_dir.glob("**/*.yaml"):
        yaml_file = yaml_file
        with open(yaml_file) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            return config


config = load_config()
# print(config)
# cleandf(loaded_file, config["AD1"])

table_config = config["AD1"]
header_config = table_config["SEX_ADOPTR"]
cell_config = header_config["category"]

print(cell_config)