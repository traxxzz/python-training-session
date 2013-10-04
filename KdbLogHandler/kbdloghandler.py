"""Handler for getting historical market data from KDB(Mercury) via deserialization of dump files.

Instantiate appropriate class with filename. Returned object acts like a
dictionary, with key-value pairs for each piece of market data.
    import kbdloghandler
    marketdata = kbdloghandler.MARKETKbdLogHandler("/home/abbath/Downloads/EUR.USD.2")


Handler can be extended by adding classes for particular markets, e.g.
NYSEKbdLogHandler, BATSKbdLogHandler etc. Each class is completely responsible for
parsing its files appropriately; see NSDQFileInfo for example.
"""

__author__ = 'abbath'

class KbdLogHandler(dict):
    "store file market data"
    def __init__(self, filename=None):
        self["name"] = filename

class NSDQKbdLogHandler(KbdLogHandler):
    "store market data from dump file in csv format of MARKET"
    def _parse(self, filename):
        "parse market data from dump file"
        self.clear()
        for line in file(filename):
            pass