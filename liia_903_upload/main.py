from pathlib import Path
import tablib

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.filters import clean
from liia_903_upload.degrade import degrade


def findfiles():
    data_dir = Path(r"C:\Users\patrick.troy\OneDrive - Social Finance Ltd\Work\Python\liia 903 upload\903")
    for p in data_dir.glob("**/*.csv"):
        yield events.StartContainer(path=p)
        yield events.EndContainer(path=p)


@streamfilter
def add_filename(event):
    return event.from_event(event, filename=str(event.path.resolve()))


def log_error(event, ex, *arg, **kwargs):
    print(ex)
    return event


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=log_error)
def parse_csv(event):
    with open(event.path, "rt") as f:
        data = tablib.import_set(f, format="csv")
        yield events.StartTable.from_event(event, headers=data.headers)
        for r_ix, row in enumerate(data):
            yield events.StartRow.from_event(event)
            for c_ix, cell in enumerate(row):
                yield events.Cell.from_event(event, r_ix=r_ix, c_ix=c_ix, label=data.headers[c_ix], cell=cell)
                yield events.EndRow.from_event(event)
        yield events.EndTable.from_event(event)


# @streamfilter(check=checks.type_check(events.Cell), fail_function=pass_event)
# def fix_label(event):
#     return event.from_event(event, label=f"column-{event.c_ix + 1}")


def main():
    stream = findfiles()
    stream = add_filename(stream)
    stream = parse_csv(stream)
    stream = clean(stream)
    stream = degrade(stream)
    for e in stream:
        print(e.as_dict())


if __name__ == "__main__":
    main()
