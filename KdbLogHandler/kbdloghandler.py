"""Handler for getting historical market data from KDB(Mercury) via parsing dump files in csv format.

Instantiate appropriate class with filename. Returned object acts like a
dictionary, with key-value pairs for each piece of market data.
    import kbdloghandler
    marketdata = kbdloghandler.MARKETKbdLogHandler("/home/abbath/Downloads/EUR.USD.2")


Handler can be extended by adding classes for particular markets, e.g.
...
"""


__author__ = 'abbath'
