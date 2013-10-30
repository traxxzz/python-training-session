"""Test for kdbloghandler.py"""

__author__ = 'Ilya Romanchenko'

import kdbloghandler

filename = "C:/Documents and Settings/romaily/My Documents/EUR.USD.2"
# filename = "/home/abbath/Downloads/EUR.USD.2"

with open(filename, "rb") as csvfile:
    marketdata = kdbloghandler.CSVKdbLogHandler(csvfile)

    print marketdata.get_next_record()
    print marketdata.get_next_record()
    print marketdata.get_next_record()
    print marketdata.get_next_record()
    for iteration in range(2):
        i = 1
        for record in marketdata:
            print record
            if i >= 2: break
            i += 1
