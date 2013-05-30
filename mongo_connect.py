#!/usr/bin/env python
# encoding: utf-8


import pymongo
import ConfigParser
import sys
import os

def open_connection(verbose=False, account='mongolab'):
    # Read the config file
    config = ConfigParser.ConfigParser()
    config.read([os.path.expanduser('~/.mongo.cfg')])

    # Connect to the server
    url = config.get(account, 'url')
    database = config.get(account, 'database')
    collection = config.get(account, 'collection')

    # Login to our account
    if verbose:
        print 'Connecting to', url
        connection = pymongo.Connection(url)
        print 'opening database ', database
        database = connection[database]
        print 'opening collection ', collection
        collection = database[collection]

    else:
        connection = pymongo.Connection(url)
        database = connection[database]
        collection = database[collection]

    return collection

if __name__ == '__main__':
    try:
        account = sys.argv[1]
    except:
        account = 'default'
    print account

    c = open_connection(verbose=True, account=account)
    print c.count()
    try:
        print c.count()
    finally:
        print 'goodbye'
