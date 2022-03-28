from datetime import datetime


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