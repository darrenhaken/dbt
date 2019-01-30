
import agate
import datetime
import pytz
import six

from agate import CastError


class ParserWrapper(object):
    """Wrap nlp and parseDT methods no the internal parser agate uses to
    provide timezone awareness when parsing datetimes. This is a bit hacky, to
    say the least...
    """
    def __init__(self, parser):
        self._parser = parser
        self._tzstash = None

    def nlp(self, inputText, sourceTime):
        # the result is a tuple of tuples, we care about the last entry of the
        # first tuple.
        result = self._parser.nlp(inputText, sourceTime=sourceTime)[0]
        _, _, _, _, matched_text = result
        # if the remaining text (after strip()) is some kind of valid timezone,
        # stash that away for later and update the matched text field to show
        # we've consumed it
        if matched_text != inputText and inputText.startswith(matched_text):
            unmatched = inputText[len(matched_text):].strip()
            try:
                self._tzstash = pytz.timezone(unmatched)
            except pytz.UnknownTimeZoneError:
                pass
            else:
                matched_text = inputText
        return ((None, None, None, None, matched_text),)

    def parseDT(self, inputText, sourceTime, tzinfo):
        value, ctx = self._parser.parseDT(
            inputText,
            sourceTime=sourceTime,
            tzinfo=tzinfo
        )
        if self._tzstash is not None:
            value = value.replace(tzinfo=self._tzstash)
        return value, ctx

    def __getattr__(self, name):
        return getattr(self._parser, name)


class DateTime(agate.data_types.DateTime):
    """An agate DateTime derivative that supports timezones. By default, agate
    will parse '2001-01-01 12:12:12.000' as a date but can't handle
    '2001-01-01 12:12:12.000 UTC'.
    """
    def __init__(self):
        super(DateTime, self).__init__(null_values=('null', ''))
        self._parser = ParserWrapper(self._parser)


DEFAULT_TYPE_TESTER = agate.TypeTester(types=[
    agate.data_types.Number(null_values=('null', '')),
    agate.data_types.TimeDelta(null_values=('null', '')),
    agate.data_types.Date(null_values=('null', '')),
    DateTime(),
    agate.data_types.Boolean(true_values=('true',),
                             false_values=('false',),
                             null_values=('null', '')),
    agate.data_types.Text(null_values=('null', ''))
])


def table_from_data(data, column_names):
    "Convert list of dictionaries into an Agate table"
    # The agate table is generated from a list of dicts, so the column order
    # from `data` is not preserved. We can use `select` to reorder the columns
    #
    # If there is no data, create an empty table with the specified columns
    if len(data) == 0:
        return agate.Table([], column_names=column_names)
    else:
        table = agate.Table.from_object(data, column_types=DEFAULT_TYPE_TESTER)
        return table.select(column_names)


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath):
    return agate.Table.from_csv(abspath, column_types=DEFAULT_TYPE_TESTER)
