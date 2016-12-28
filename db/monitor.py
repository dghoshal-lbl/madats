#!/usr/bin/env python

from pymongo import MongoClient
import pymongo
from db.connection import Connection
import time
import sys
import os
from tabulate import tabulate
import curses
#import signal
import collections

class DbMonitor():
    def __init__(self, host='localhost', port=27017, db='vds', collection='tasks'):
        self._conn_ = Connection(host, port)
        self._db_ = db
        self._coll_ = collection
        #signal.signal(signal.SIGINT, self.signal_handler)

    #def signal_handler(self, signal, frame):
    #    sys.exit(0)

    def add_resultset(self, resultset, fields, doc):
        rec = []
        for field in fields:
            if fields[field] !=False:
                rec.append(doc[field])
        resultset.append(rec)

    def consolidate_results(self, resultset, fields, doc):
        rec = []
        record = {}
        for k in doc:
            if k in fields:
                if fields[k] !=False:
                    record[k] = doc[k]
            else:
                if not (k.endswith('time') or k.endswith('id') or k=='dependencies'):
                    if 'misc' in record:
                        record['misc'] = record['misc'] + ';' + k + ':' + str(doc[k])
                    else:
                        record['misc'] = k + ':'+ str(doc[k])
        if 'misc' not in record:
            record['misc'] = ''

        sorted_rec = collections.OrderedDict(sorted(record.items(), key=lambda t:t[0]))
        for k, v in sorted_rec.items():
            rec.append(v)
        resultset.append(rec)
        
    def count(self):
        conn = self._conn_.connect()
        db = conn[self._db_]

        coll = db[self._coll_]
        result = coll.find({}).count()
        conn.close()
        #print("Number of records in collection({}): {}".format(self._coll_, result))
        return result

    def query(self):
        conn = self._conn_.connect()
        db = conn[self._db_]

        #field_names = ['workflow_id', 'task_id', 'type', 'command', 'params', 'submission_time', 'start_time', 'end_time', 'status']
        field_names = ['workflow_id', 'type', 'command', 'params', 'status']
        #order = (field_names[0], pymongo.ASCENDING)

        fields = {}
        cond = {}
        
        coll = db[self._coll_]
        
        for field in field_names:
            fields[field.strip()] = True
        fields['_id'] = False

        header = []
        for field in fields:
            if fields[field] != False:
                header.append(field)
        
        header.append('misc')
        header.sort()

        resultset = []
        for doc in coll.find().limit(20): #.sort(order):
            self.consolidate_results(resultset, fields, doc)

        results_table = tabulate(resultset, headers=header, tablefmt="orgtbl")
        
        conn.close()

        return results_table

    def monitor(self):
        scr = curses.initscr()
        try:
            while True:
                results_table = self.query()
                scr.erase()
                scr.addstr(results_table)
                scr.refresh()
                time.sleep(5)
        finally:
            curses.endwin()


    def find_data_tasks(self):
        conn = self._conn_.connect()
        db = conn[self._db_]
        coll = db[self._coll_]
        records = []
        for doc in coll.find({'type': 'DATA', 'status':{'$ne': 'COMPLETED'}}):
            records.append(doc)
        conn.close()
        return records
        

    def check_task_status(self, task_id):
        conn = self._conn_.connect()
        db = conn[self._db_]
        fields = {'_id': False, 'status': True}
        coll = db[self._coll_]
        resultset = []
        for doc in coll.find({'task_id': task_id}, projection=fields):
            self.add_resultset(resultset, fields, doc)
        conn.close()
        return resultset[0][0]

if __name__ == '__main__':
    db_monitor = DbMonitor()
    db_monitor.monitor()
