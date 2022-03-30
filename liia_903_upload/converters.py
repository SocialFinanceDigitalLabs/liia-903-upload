from datetime import datetime
import datetime


def to_category(string, categories):
    for code in categories:
        if str(string).lower() == str(code['code']).lower():
            return code['code']
        elif 'name' in code:
            if str(code['name']).lower() in str(string).lower():
                return code['code']
    return 'Not in proper format: {}'.format(string)
    # If time, add here the matching report


def to_date(string, dateformat):
    string = string.replace('/', '-')
    try:
        datetime.strptime(string, dateformat) # Check this is possible
    except:
        string = 'Not in proper format: {}'.format(string)
    return string
    # If time, add here the matching report


def to_short_postcode(string):
    """
    Remove all whitespace from postcodes and the last two digits for anonymity
    """
    try:
        string = string.strip()
        string = string[:-2]
    except:
        string = "Not in proper format"
    return string


def to_month_only_dob(date):
    """
    Convert dates of birth into month and year of birth for anonymity
    """
    try:
        dob = date.strftime("%Y-%m")
    except:
        dob = "Not in proper format"
    return dob
