#!/usr/bin/env python2
"""
Tool for directory release comparing.

    Sometimes after release needed to check , that directory tree for release is same as for previous
    release or with some extra files or some missing files. So QA team wants an utility, that you can
    run and compare directories for theirs content. This tool should compare content recursively.

Example:
    comparertool.py ./release_1/./release_2/.

Output of utility:
    If diff founded between tree of directories, print diffs. Otherwise return what????

Optional:
    Add keys to command line which files to skip(by mask).
    Add keys to command line to dump directory tree.
"""

__author__ = "Ilya Romanchenko (ilya.romanchenko@gmail.com)"

import argparse
import sys

def args_parse():
    """parse command line parameters"""
    parser = argparse.ArgumentParser(description='Tool for directory release comparing.')
    parser.add_argument("dir1", help="")
    parser.parse_args()

class ComparerError(Exception):
    """Base class for exceptions in comparertool.py"""
    pass
class ComparerImportError(ComparerError, ImportError): pass

class Comparer(object):
    """Compare trees of two directories."""
    def __init__(self):
        pass

def usage():
    print __doc__

def main():
    args_parse()

if __name__ == "__main__":
    main()
else:
    raise ComparerImportError('This module is not intended to be imported')