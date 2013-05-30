#!/usr/bin/env python
# encoding: utf-8

import sys
from collections import defaultdict
import mongo_connect as mc




def key_count(collection):

    sys.stderr.write('loading collection...')
    a = list(collection.find({}))
    sys.stderr.write('loaded %d records\n' % len(a))
    sys.stderr.write('counting keys...')

    keycount = defaultdict(int)

    for i, doc in enumerate(a):
        for key in doc.keys():
            keycount[key] += 1
        if i % 1000 == 0:
            print i, doc

    return dict(keycount)

if __name__ == '__main__':
    c = mc.open_connection(account='mongolab')
    keycount = key_count(c)



