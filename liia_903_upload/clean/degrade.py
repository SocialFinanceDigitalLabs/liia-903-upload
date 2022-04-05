from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.clean.converters import to_short_postcode, to_month_only_dob


@streamfilter(check=lambda x: x.get('header') in ['HOME_POST', 'PL_POST'],fail_function=pass_event,
              error_function=pass_event)
def degrade_postcodes(event):
    """
    Convert all values that should be postcodes to shorter postcodes
    """
    text = to_short_postcode(event.cell)
    return event.from_event(event, cell=text)


@streamfilter(check=lambda x: x.get('header') in ['DOB', 'MC_DOB'], fail_function=pass_event,
              error_function=pass_event)
def degrade_dob(event):
    """
    Convert all values that should be dates of birth to months and year of birth
    """
    text = to_month_only_dob(event.cell)
    return event.from_event(event, cell=text)


def degrade(stream):
    """
    Compile the degrading functions
    """
    stream = degrade_postcodes(stream)
    stream = degrade_dob(stream)
    return stream
