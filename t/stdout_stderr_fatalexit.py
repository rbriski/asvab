#!/usr/local/bin/python

import sys

print "fatalexit prints to stdout"

sys.stderr.write('fatalexit prints to stderr\n')

print 1/0
