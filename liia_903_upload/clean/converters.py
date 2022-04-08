from datetime import datetime
import re


def to_category(string, categories):
    """
    Matches a string to a category based on categories given in a config file
    the config file should contain a dictionary for each category for this function to loop through
    return blank if no categories found
    """
    for code in categories:
        if str(string).lower() == str(code['code']).lower():
            return code['code']
        elif 'name' in code:
            if str(code['name']).lower() in str(string).lower():
                return code['code']
    return ""


def to_date(string, dateformat):
    """
    Convert a string to a date based on the dateformat e.g. "%d/%m/%Y"
    return blank if not in the right format
    """
    try:
        string = datetime.strptime(string, dateformat).date() # Check this is possible
    except ValueError:
        string = ""
    except TypeError:
        string = ""
    return string


def to_integer(string, config):
    """
    Convert any strings that should be integers based on the config into integers
    """
    if config == "integer":
        try:
            string = int(string)
        except ValueError:
            string = ""
        except TypeError:
            string = ""
        return string
    else:
        return string


def check_postcode(string):
    """
    Checks that the postcodes are in the right format
    returns blank if not in the right format
    """
    try:
        match = re.search(r"[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}", string, re.IGNORECASE)
        string = match.group(0)
    except ValueError:
        string = ""
    except TypeError:
        string = ""
    except AttributeError:
        string = ""
    return string

def to_short_postcode(string):
    """
    Remove whitespace from the beginning and end of postcodes and the last two digits for anonymity
    return blank if not in the right format
    """
    try:
        string = string.strip()
        string = string[:-2]
    except ValueError:
        string = ""
    except TypeError:
        string = ""
    return string


def to_month_only_dob(date):
    """
    Convert dates of birth into month and year of birth starting from 1st of each month for anonymity
    return blank if not in the right format
    """
    try:
        date = date.strftime("%Y-%m-01")
    except ValueError:
        date = ""
    except TypeError:
        date = ""
    except AttributeError:
        date = ""
    return date
