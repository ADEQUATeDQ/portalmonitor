__author__ = 'jumbrich'

import argparse

def name():
    return 'DB'

def setupCLI(pa):
    pa.add_argument('DB', type=int, help='bar help')

def cli(argv):
    print argv