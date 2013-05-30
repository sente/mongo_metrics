#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import pprint
import tablib
from collections import defaultdict
import mongo_connect as mc


LIMIT = 5000 #limit the number of records..

def save_dataset(dataset, filename):
    """
    save the <dataset> to disk, the file format is a function of <filename>'s
    extension.
    """
    extension = filename.split(".")[-1]
    if extension not in ('csv', 'html', 'xls', 'json', 'xlsx'):
        sys.stderr.write("invalid filename: %s filename\n" % filename)

    print extension

    try:
        open(filename,'w').write(ds.__getattribute__(extension))
        sys.stdout.write("saved %s \n" % filename)
    except Exception, e:
        sys.stderr.write(str(e))



if __name__ == '__main__':

    c = mc.open_connection('mongolab')


    sys.stderr.write('loading events..')
    a = list(c.find({}).limit(LIMIT))
    sys.stderr.write('loaded %d events\n' % len(a))

    unique_keys = set()

    for doc in a:
        for k in doc.keys():
            unique_keys.add(k)


    headers = list(map(str,list(unique_keys)))
    pprint.pprint(headers)

    ds = tablib.Dataset(headers=headers)

    for i,doc in enumerate(a[0:LIMIT]):
        vals = [str(doc.get(h,'')) for h in headers]
        ds.append(vals)
        if i % 1000 == 0:
            sys.stderr.write('loaded %d vals into the dataset\n' % i)


    save_dataset(ds, 'out/ds_test.csv')
    save_dataset(ds, 'out/ds_test.html')

