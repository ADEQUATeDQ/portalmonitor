__author__ = 'jumbrich'
import os
import argparse
import sys
import logging
import logging.config
from db import cli as dbcli
import init as initcli
import fetch as fetchcli


submodules=[dbcli, initcli, fetchcli]

def start ():
    pa = argparse.ArgumentParser(description='Open Portal Watch toolset.',prog='ODPW')

    print 'Here'

    pa.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING,
    )
    pa.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )
    pa.add_argument('--host', help="DB host", dest='dbhost')

    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help='a help')
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)

    args = pa.parse_args()

    logging.basicConfig(level=args.loglevel)

    args.func(args)

if __name__ == "__main__":
    start()
