from pathlib import Path
import tablib

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.clean.filters import clean
from liia_903_upload.clean.populate import populate
from liia_903_upload.clean.degrade import degrade
from liia_903_upload.clean.file_creator import coalesce_row, create_tables, save_tables
from liia_903_upload.clean.config import inherit_table_name, add_table_name, load_config, match_config_to_cell


def findfiles():
    """
    Locate the csv files within the given directory
    """
    data_dir = Path(r"C:\Users\patrick.troy\OneDrive - Social Finance Ltd\Work\Python\liia 903 upload\LDS")
    for p in data_dir.glob("**/Inputs/*.csv"):
        yield events.StartContainer(path=p)
        yield events.EndContainer(path=p)


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_filename(event):
    """
    Return the filename including the path to that file, so we can extract year and LA name data
    """
    return event.from_event(event, filename=str(event.path.resolve()))


def log_error(event, ex, *arg, **kwargs):
    return event


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=log_error)
def parse_csv(event):
    """
    Parse the csv and return the row number, column number, header name and cell value
    """
    with open(event.path, "rt") as f:
        data = tablib.import_set(f, format="csv")
        yield events.StartTable.from_event(event, headers=data.headers)
        for r_ix, row in enumerate(data):
            yield events.StartRow.from_event(event)
            for c_ix, cell in enumerate(row):
                yield events.Cell.from_event(event, r_ix=r_ix, c_ix=c_ix, header=data.headers[c_ix], cell=cell)
            yield events.EndRow.from_event(event)
        yield events.EndTable.from_event(event)


def main():
    config = load_config()
    stream = findfiles()
    stream = add_filename(stream)
    stream = populate(stream)
    stream = parse_csv(stream)
    stream = add_table_name(stream)
    stream = inherit_table_name(stream)
    stream = match_config_to_cell(stream, config=config)
    stream = clean(stream)
    stream = degrade(stream)
    stream = coalesce_row(stream)
    stream = create_tables(stream)
    save_tables(stream)


if __name__ == "__main__":
    main()
