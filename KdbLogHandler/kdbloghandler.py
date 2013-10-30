"""Handler for getting historical market data from KDB(Mercury) via parsing dump files.

Instantiate appropriate class with file object. Returned object has method get_next_record()
returning dict with key-value pairs for each piece of market data. In addition the object is iterable.

    import kdbloghandler
    with open(filename, "rb") as csvfile:
        marketdata = kdbloghandler.CSVKbdLogHandler(csvfile)

        record = marketdata.get_next_record()   # In such case of usage the object acts like a generator.
                                                # It preserves last position in file.

        for record in marketdata:               # The object acts like an iterator.
            proceed(record)                     # It starts from the beginning each time.


Handler can be extended by adding classes for particular file types, e.g. XMLKbdLogHandler, JSONKbdLogHandler etc.
Each class is completely responsible for parsing its files appropriately; see CSVFileInfo for example.
All extending classes should be inherited from KdbLogHandler. All of them have to implement get_next_record() method.
"""

__author__ = 'Ilya Romanchenko'

import csv

class KdbLogHandler(object):
    """Store market data from dump file."""
    def __init__(self, csvfile=None):
        self.csvfile = csvfile

    def get_next_record(self):
        raise KdbLogHandlerNotImplementedError("Subclasses of KdbLogHandler must implement get_next_record() method")

    def __iter__(self):
        while True:
            yield self.get_next_record()

class CSVKdbLogHandler(KdbLogHandler):
    """Provide comfortable access to KDB(Mercury) dump files in csv format."""

    def __init__(self, csvfile):
        super(CSVKdbLogHandler, self).__init__(csvfile)
        self.reader = self.__get_csv_reader(csvfile)
        self.rowDict = {}

    def __iter__(self):
        self.reader = self.__get_csv_reader(self.csvfile)
        self.rowDict = {}
        return super(CSVKdbLogHandler, self).__iter__()

    def __get_csv_reader(self, csvfile):
        """read market data from dump file and return generator object."""
        try:
            csvfile.seek(0)
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
        except csv.Error, e:
            raise KdbLogHandlerCSVError('Bad file: "{}": {}'.format(self.csvfile.name, e))
        else:
            csvfile.seek(0)
        return csv.DictReader(csvfile, dialect=dialect, delimiter=',')

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
                raise KdbLogHandlerKeyError('Bad file: "{}" does not contain required column {}'.format(self.csvfile.name, e))
        return rowDict

    def get_next_record(self):
        """Return a piece of market data for all levels joint into one compound dictionary."""
        resultDict = self.rowDict
        while self.reader:
            try:
                rowDict = self.__restruct(self.reader.next())
                if None in rowDict.keys() or None in rowDict.values():
                    raise csv.Error('The row read has different columns count than the column names sequence')
            except csv.Error, e:
                raise KdbLogHandlerCSVError('Bad file: "{}", line {}:\n {}'.format(self.csvfile.name, self.reader.line_num, e))
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
