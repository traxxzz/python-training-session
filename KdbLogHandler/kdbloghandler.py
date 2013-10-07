"""Handler for getting historical market data from KDB(Mercury) via parsing dump files.

Instantiate appropriate class with filename. Returned object has method get_next_record()
returning dict with key-value pairs for each piece of market data. Moreover it is iterable.

    import kdbloghandler
    marketdata = kdbloghandler.CSVKbdLogHandler("/home/abbath/Downloads/EUR.USD.2")

    record = marketdata.get_next_record()   # In such case of usage the object acts like a generator.
                                            # It constantly remembers the last position in the file.

    for record in marketdata:               # The object acts like an iterator.
        proceed(record)                     # It starts from the beginning each time.


Handler can be extended by adding classes for particular file types, e.g. XMLKbdLogHandler.
Each class is completely responsible for parsing its files appropriately; see CSVFileInfo for example.
KdbLogHandler is a kind of interface. Extending classes have to implement his methods.
"""

__author__ = 'Ilya Romanchenko'

class KdbLogHandler(object):
    """store market data from dump file"""
    def __init__(self, filename=None):
        self.filename = filename

    def get_next_record(self):
        raise NotImplementedError("Subclasses should implement get_next_record()")

    def __iter__(self):
        return iter(self.get_next_record, None)

class CSVKdbLogHandler(KdbLogHandler):
    """Provide comfortable access to KDB(Mercury) dump files in csv format"""

    def __init__(self, filename):
        super(CSVKdbLogHandler, self).__init__(filename)
        self.gen = None
        self.__repeat = None
        self.columnNamesList = self.__get_column_names()

    def __get_column_names(self):
        """Initiate generator and return list of column headings"""
        if not self.gen:
            self.gen = self.__parse(self.filename)
            return self.gen.next()
        else:
            return self.columnNamesList

    def __parse(self, filename):
        """read market data from dump file and return generator object"""
        with open(filename, "r") as dump:
            for line in dump:
                rowDict = line.strip("\r\n").split(",")
                while (yield rowDict):
                    continue

    def __merge(self, lines):
        """Make one joint dict. Not for the faint of heart"""
        jointDict = {k: v for k, v in zip(self.columnNamesList, lines[0]) if k not in self.columnNamesList[7:14]}
        for line in lines:
            jointDict['bid{}'.format(int(line[7])-1)] = {k: v for k, v in zip(self.columnNamesList, line)[8:11]}
            jointDict['ask{}'.format(int(line[7])-1)] = {k: v for k, v in zip(self.columnNamesList, line)[11:14]}
        return jointDict

    def get_next_record(self):
        """Return piece of market data for all levels joint in one dictionary"""
        lines = []
        while True:
            lines.append(self.gen.send(self.__repeat))
            if lines[1:] and int(lines[-2][7]) != int(lines[-1][7])-1 and lines[-2][-1] != lines[-1][-1]:
                del lines[-1]
                self.__repeat = True
                break
            self.__repeat = None
        return self.__merge(lines)

    def __iter__(self):
        self.__repeat = None
        self.gen = self.__parse(self.filename)
        self.gen.next()
        return super(CSVKdbLogHandler, self).__iter__()

# Debug
if __name__ == '__main__':
    marketdata = CSVKdbLogHandler("C:/Documents and Settings/romaily/My Documents/EUR.USD.2")
    i = 1
    for record in marketdata:
        print record
        if i >= 3: break
        i += 1
    i = 1
    for record in marketdata:
        print record
        if i >= 3: break
        i += 1
    print marketdata.get_next_record()
    print marketdata.get_next_record()

# Output on my computer
"""
/usr/bin/python2.7 /home/abbath/PycharmProjects/python-training-session/KdbLogHandler/kdbloghandler.py
{'exchQuantumTime': '', 'cond': 'Dealable', 'exchTime': '13:00:00.250', 'bid9': {'bidSize': '12000000', 'bidPrice': '1.3499', 'bidCount': ''}, 'bid8': {'bidSize': '6000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid7': {'bidSize': '11000000', 'bidPrice': '1.35', 'bidCount': ''}, 'bid6': {'bidSize': '6000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'bid5': {'bidSize': '11000000', 'bidPrice': '1.3501', 'bidCount': ''}, 'bid4': {'bidSize': '6000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'bid3': {'bidSize': '10000000', 'bidPrice': '1.3502', 'bidCount': ''}, 'bid2': {'bidSize': '7000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'bid1': {'bidSize': '9000000', 'bidPrice': '1.3503', 'bidCount': ''}, 'bid0': {'bidSize': '1000000', 'bidPrice': '1.35035', 'bidCount': ''}, 'exchDate': '2013.09.26', 'globalSeqNum': '113443286238', 'seqNum': '2464310', 'sym': 'EURUSD', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask5': {'askCount': '', 'askPrice': '1.3507', 'askSize': '16000000'}, 'ask4': {'askCount': '', 'askPrice': '1.35065', 'askSize': '2000000'}, 'ask7': {'askCount': '', 'askPrice': '1.3508', 'askSize': '11000000'}, 'ask6': {'askCount': '', 'askPrice': '1.35075', 'askSize': '2000000'}, 'ask1': {'askCount': '', 'askPrice': '1.3505', 'askSize': '12000000'}, 'ask0': {'askCount': '', 'askPrice': '1.35045', 'askSize': '7000000'}, 'ask3': {'askCount': '', 'askPrice': '1.3506', 'askSize': '14000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35055', 'askSize': '3000000'}, 'ask9': {'askCount': '', 'askPrice': '1.3509', 'askSize': '13000000'}, 'ask8': {'askCount': '', 'askPrice': '1.35085', 'askSize': '2000000'}, 'time': '13:00:00.251'}
{'ask4': {'askCount': '', 'askPrice': '1.35095', 'askSize': '84000000'}, 'bid1': {'bidSize': '33000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'ask1': {'askCount': '', 'askPrice': '1.35065', 'askSize': '38000000'}, 'exchTime': '13:00:00.250', 'ask3': {'askCount': '', 'askPrice': '1.35085', 'askSize': '69000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35075', 'askSize': '56000000'}, 'seqNum': '2464311', 'bid4': {'bidSize': '83000000', 'bidPrice': '1.34985', 'bidCount': ''}, 'bid3': {'bidSize': '67000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid2': {'bidSize': '50000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'sym': 'EURUSD', 'bid0': {'bidSize': '17000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'exchQuantumTime': '', 'cond': 'Spread', 'globalSeqNum': '113443286243', 'exchDate': '2013.09.26', 'time': '13:00:00.251', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask0': {'askCount': '', 'askPrice': '1.35055', 'askSize': '22000000'}}
{'ask4': {'askCount': '', 'askPrice': '1.353', 'askSize': '250000000'}, 'bid1': {'bidSize': '100000000', 'bidPrice': '1.3498', 'bidCount': ''}, 'ask1': {'askCount': '', 'askPrice': '1.351', 'askSize': '100000000'}, 'exchTime': '13:00:01.995', 'ask3': {'askCount': '', 'askPrice': '1.3515', 'askSize': '200000000'}, 'ask2': {'askCount': '', 'askPrice': '1.3512', 'askSize': '150000000'}, 'seqNum': '2464333', 'bid4': {'bidSize': '250000000', 'bidPrice': '1.3477', 'bidCount': ''}, 'bid3': {'bidSize': '200000000', 'bidPrice': '1.349', 'bidCount': ''}, 'bid2': {'bidSize': '150000000', 'bidPrice': '1.3493', 'bidCount': ''}, 'sym': 'EURUSD', 'bid0': {'bidSize': '50000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'exchQuantumTime': '', 'cond': 'Regular', 'globalSeqNum': '113443286387', 'exchDate': '2013.09.26', 'time': '13:00:01.997', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask0': {'askCount': '', 'askPrice': '1.3507', 'askSize': '50000000'}}
{'exchQuantumTime': '', 'cond': 'Dealable', 'exchTime': '13:00:00.250', 'bid9': {'bidSize': '12000000', 'bidPrice': '1.3499', 'bidCount': ''}, 'bid8': {'bidSize': '6000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid7': {'bidSize': '11000000', 'bidPrice': '1.35', 'bidCount': ''}, 'bid6': {'bidSize': '6000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'bid5': {'bidSize': '11000000', 'bidPrice': '1.3501', 'bidCount': ''}, 'bid4': {'bidSize': '6000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'bid3': {'bidSize': '10000000', 'bidPrice': '1.3502', 'bidCount': ''}, 'bid2': {'bidSize': '7000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'bid1': {'bidSize': '9000000', 'bidPrice': '1.3503', 'bidCount': ''}, 'bid0': {'bidSize': '1000000', 'bidPrice': '1.35035', 'bidCount': ''}, 'exchDate': '2013.09.26', 'globalSeqNum': '113443286238', 'seqNum': '2464310', 'sym': 'EURUSD', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask5': {'askCount': '', 'askPrice': '1.3507', 'askSize': '16000000'}, 'ask4': {'askCount': '', 'askPrice': '1.35065', 'askSize': '2000000'}, 'ask7': {'askCount': '', 'askPrice': '1.3508', 'askSize': '11000000'}, 'ask6': {'askCount': '', 'askPrice': '1.35075', 'askSize': '2000000'}, 'ask1': {'askCount': '', 'askPrice': '1.3505', 'askSize': '12000000'}, 'ask0': {'askCount': '', 'askPrice': '1.35045', 'askSize': '7000000'}, 'ask3': {'askCount': '', 'askPrice': '1.3506', 'askSize': '14000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35055', 'askSize': '3000000'}, 'ask9': {'askCount': '', 'askPrice': '1.3509', 'askSize': '13000000'}, 'ask8': {'askCount': '', 'askPrice': '1.35085', 'askSize': '2000000'}, 'time': '13:00:00.251'}
{'ask4': {'askCount': '', 'askPrice': '1.35095', 'askSize': '84000000'}, 'bid1': {'bidSize': '33000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'ask1': {'askCount': '', 'askPrice': '1.35065', 'askSize': '38000000'}, 'exchTime': '13:00:00.250', 'ask3': {'askCount': '', 'askPrice': '1.35085', 'askSize': '69000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35075', 'askSize': '56000000'}, 'seqNum': '2464311', 'bid4': {'bidSize': '83000000', 'bidPrice': '1.34985', 'bidCount': ''}, 'bid3': {'bidSize': '67000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid2': {'bidSize': '50000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'sym': 'EURUSD', 'bid0': {'bidSize': '17000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'exchQuantumTime': '', 'cond': 'Spread', 'globalSeqNum': '113443286243', 'exchDate': '2013.09.26', 'time': '13:00:00.251', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask0': {'askCount': '', 'askPrice': '1.35055', 'askSize': '22000000'}}
{'ask4': {'askCount': '', 'askPrice': '1.353', 'askSize': '250000000'}, 'bid1': {'bidSize': '100000000', 'bidPrice': '1.3498', 'bidCount': ''}, 'ask1': {'askCount': '', 'askPrice': '1.351', 'askSize': '100000000'}, 'exchTime': '13:00:01.995', 'ask3': {'askCount': '', 'askPrice': '1.3515', 'askSize': '200000000'}, 'ask2': {'askCount': '', 'askPrice': '1.3512', 'askSize': '150000000'}, 'seqNum': '2464333', 'bid4': {'bidSize': '250000000', 'bidPrice': '1.3477', 'bidCount': ''}, 'bid3': {'bidSize': '200000000', 'bidPrice': '1.349', 'bidCount': ''}, 'bid2': {'bidSize': '150000000', 'bidPrice': '1.3493', 'bidCount': ''}, 'sym': 'EURUSD', 'bid0': {'bidSize': '50000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'exchQuantumTime': '', 'cond': 'Regular', 'globalSeqNum': '113443286387', 'exchDate': '2013.09.26', 'time': '13:00:01.997', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask0': {'askCount': '', 'askPrice': '1.3507', 'askSize': '50000000'}}
{'exchQuantumTime': '', 'cond': 'Dealable', 'exchTime': '13:00:02.496', 'bid9': {'bidSize': '12000000', 'bidPrice': '1.3499', 'bidCount': ''}, 'bid8': {'bidSize': '6000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid7': {'bidSize': '11000000', 'bidPrice': '1.35', 'bidCount': ''}, 'bid6': {'bidSize': '6000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'bid5': {'bidSize': '11000000', 'bidPrice': '1.3501', 'bidCount': ''}, 'bid4': {'bidSize': '6000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'bid3': {'bidSize': '10000000', 'bidPrice': '1.3502', 'bidCount': ''}, 'bid2': {'bidSize': '7000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'bid1': {'bidSize': '9000000', 'bidPrice': '1.3503', 'bidCount': ''}, 'bid0': {'bidSize': '1000000', 'bidPrice': '1.35035', 'bidCount': ''}, 'exchDate': '2013.09.26', 'globalSeqNum': '113443286407', 'seqNum': '2464337', 'sym': 'EURUSD', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask5': {'askCount': '', 'askPrice': '1.3507', 'askSize': '16000000'}, 'ask4': {'askCount': '', 'askPrice': '1.35065', 'askSize': '2000000'}, 'ask7': {'askCount': '', 'askPrice': '1.3508', 'askSize': '11000000'}, 'ask6': {'askCount': '', 'askPrice': '1.35075', 'askSize': '2000000'}, 'ask1': {'askCount': '', 'askPrice': '1.3505', 'askSize': '12000000'}, 'ask0': {'askCount': '', 'askPrice': '1.35045', 'askSize': '6000000'}, 'ask3': {'askCount': '', 'askPrice': '1.3506', 'askSize': '14000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35055', 'askSize': '3000000'}, 'ask9': {'askCount': '', 'askPrice': '1.3509', 'askSize': '13000000'}, 'ask8': {'askCount': '', 'askPrice': '1.35085', 'askSize': '2000000'}, 'time': '13:00:02.498'}
{'ask4': {'askCount': '', 'askPrice': '1.35095', 'askSize': '83000000'}, 'bid1': {'bidSize': '33000000', 'bidPrice': '1.35015', 'bidCount': ''}, 'ask1': {'askCount': '', 'askPrice': '1.35065', 'askSize': '37000000'}, 'exchTime': '13:00:02.497', 'ask3': {'askCount': '', 'askPrice': '1.35085', 'askSize': '68000000'}, 'ask2': {'askCount': '', 'askPrice': '1.35075', 'askSize': '55000000'}, 'seqNum': '2464338', 'bid4': {'bidSize': '83000000', 'bidPrice': '1.34985', 'bidCount': ''}, 'bid3': {'bidSize': '67000000', 'bidPrice': '1.34995', 'bidCount': ''}, 'bid2': {'bidSize': '50000000', 'bidPrice': '1.35005', 'bidCount': ''}, 'sym': 'EURUSD', 'bid0': {'bidSize': '17000000', 'bidPrice': '1.35025', 'bidCount': ''}, 'exchQuantumTime': '', 'cond': 'Spread', 'globalSeqNum': '113443286411', 'exchDate': '2013.09.26', 'time': '13:00:02.498', 'date': '2013.09.26', 'exch': 'EBSUSDEUN', 'ask0': {'askCount': '', 'askPrice': '1.35055', 'askSize': '21000000'}}

Process finished with exit code 0
"""