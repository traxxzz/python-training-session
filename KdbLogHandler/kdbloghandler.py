"""Handler for getting historical market data from KDB(Mercury) via parsing dump files.

Instantiate appropriate class with filename. Returned object has method get_next_record()
returning dict with key-value pairs for each piece of market data. In addition the object is iterable.

    import kdbloghandler
    marketdata = kdbloghandler.CSVKbdLogHandler("/home/abbath/Downloads/EUR.USD.2")

    record = marketdata.get_next_record()   # In such case of usage the object acts like a generator.
                                            # It preserves last position in file.

    for record in marketdata:               # The object acts like an iterator.
        proceed(record)                     # It starts from the beginning each time.


Handler can be extended by adding classes for particular file types, e.g. XMLKbdLogHandler, JSONKbdLogHandler etc.
Each class is completely responsible for parsing its files appropriately; see CSVFileInfo for example.
KdbLogHandler is a kind of interface. Extending classes have to implement get_next_record() method.
"""

__author__ = 'Ilya Romanchenko'

import csv

class KdbLogHandler(object):
    """Store market data from dump file."""
    def __init__(self, filename=None):
        self.filename = filename

    def get_next_record(self):
        raise KdbLogHandlerNotImplementedError("Subclasses of KdbLogHandler must implement get_next_record() method")

    def __iter__(self):
        while True:
            yield self.get_next_record()

class CSVKdbLogHandler(KdbLogHandler):
    """Provide comfortable access to KDB(Mercury) dump files in csv format."""

    def __init__(self, filename):
        super(CSVKdbLogHandler, self).__init__(filename)
        self.reader = self.__open(filename)
        self.rowDict = {}

    def __iter__(self):
        self.reader = self.__open(self.filename)
        self.rowDict = {}
        return super(CSVKdbLogHandler, self).__iter__()

    def __open(self, filename):
        """read market data from dump file and return generator object."""
        with open(filename, "rb") as csvfile:
            try:
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
            except csv.Error, e:
                raise KdbLogHandlerCSVError('Bad file: "{}": {}'.format(self.filename, e))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect, delimiter=',')
            try:
                for row in reader:
                    if None in row.keys() or None in row.values():
                        raise csv.Error('The row read has different columns count than the column names sequence')
                    yield row
            except csv.Error, e:
                raise KdbLogHandlerCSVError('Bad file: "{}", line {}:\n {}'.format(self.filename, reader.line_num, e))

    def __restruct(self, rowDict):
        """Flattened dict from row of csv ==> desired structure with nested dicts."""
        if rowDict:
            try:
                rowDict['bid{}'.format(rowDict['level'])] = {'bidCount': rowDict['bidCount'],
                                                             'bidSize': rowDict['bidSize'],
                                                             'bidPrice': rowDict['bidPrice']}

                rowDict['ask{}'.format(rowDict['level'])] = {'askCount': rowDict['askCount'],
                                                             'askSize': rowDict['askSize'],
                                                             'askPrice': rowDict['askPrice']}
            except KeyError, e:
                raise KdbLogHandlerKeyError('Bad file: "{}" does not contain required column {}'.format(self.filename, e))
        return rowDict

    def get_next_record(self):
        """Return a piece of market data for all levels joint into one compound dictionary."""
        resultDict = self.rowDict
        while self.reader:
            try:
                rowDict = self.__restruct(self.reader.next())
            except StopIteration:
                self.reader = None
                break
            if resultDict and rowDict['level'] == '1':
                self.rowDict = rowDict
                break
            resultDict.update(rowDict)
        else:
            raise StopIteration
        return resultDict

class KdbLogHandlerError(Exception):
    """Base class for exceptions in kdbloghandler."""
    pass

class KdbLogHandlerNotImplementedError(KdbLogHandlerError):
    pass

class KdbLogHandlerKeyError(KdbLogHandlerError):
    pass

class KdbLogHandlerCSVError(KdbLogHandlerError):
    pass
