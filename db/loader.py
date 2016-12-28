#!/usr/bin/env python

from core.task import Task, DataTask
from core.vds_coordinator import DAGManagement
from pymongo import MongoClient
import pymongo
from db.connection import Connection
import time
import sys
import os
from tabulate import tabulate
import socket
import datetime
from db.monitor import DbMonitor

class DbLoader():
    def __init__(self, host='localhost', port=27017, db='vds', collection='tasks'):
        self._conn_ = Connection(host, port)
        self._db_ = db
        self._coll_ = collection

    def truncate(self, collections=[]):
        conn = self._conn_.connect()
        db = conn[self._db_]

        if len(collections) == 0:
            coll = db[self._coll_]
            result = coll.delete_many({})
        #print("Number of records deleted: {}".format(result.deleted_count))
        else:
            for collection in collections:
                coll = db[collection]
                result = coll.delete_many({})

    '''
    inserts tasks to mongo (for now)
    fields: id, name, type, command, params, dependencies, submission_time, start_time, end_time, deadline, status [(p)ending/(s)ubmitted/(r)unning/(c)ompleted/(f)ailed]
    '''
    def insert(self, vds_dag):
        #print "Connecting to MongoDB..."
        conn = self._conn_.connect()
        db = conn[self._db_]

        coll = db[self._coll_]
        
        task_map = {}
        dag_mgmt = DAGManagement(vds_dag)
        task_order = dag_mgmt.batch_execution_order()
        docs = []

        workflow_coll = db['workflows']
        
        db_monitor = DbMonitor(collection='workflows')
        nrecs = db_monitor.count()
        workflow_id = nrecs
        rec = {'id': workflow_id, 'nstages': len(task_order), 'submission_time': '', 'start_time': '', 'end_time': ''}
        workflow_coll.insert(rec)

        for task in task_order:
            rec = {}
            rec['workflow_id'] = workflow_id
            rec['task_id'] = str(task.__id__)
            if task.type == Task.COMPUTE:
                rec['type'] = 'COMPUTE'
                rec['status'] = 'PENDING'
            else:
                rec['type'] = 'DATA'
                rec['status'] = 'NOT_AVAILABLE'

            rec['command'] = task.command
            params = ' '.join(task.get_remapped_params())
            rec['params'] = params
            deps = [str(t.__id__) for t in dag_mgmt.predecessors(task)]
            rec['dependencies'] = ','.join(deps)
            rec['submission_time'] = '' # datetime.datetime.fromtimestamp()
            rec['start_time'] = ''
            rec['end_time'] = ''
            for k in task.args:
                rec[k] = task.args[k]
            docs.append(rec)
        
        start_time = time.time()
        if len(docs) > 0:
            result = coll.insert_many(docs)
            #print("Number of records inserted: {}".format(len(result.inserted_ids)))
            
        end_time = time.time()
        #print("Time to insert: %s seconds" % (end_time - start_time))

        conn.close()

        return workflow_id


    def update_status(self, task_id, status):
        conn = self._conn_.connect()
        db = conn[self._db_]

        coll = db[self._coll_]
        coll.update({'task_id': task_id}, {'$set':{'status': status}}, upsert=False)
        conn.close()
        
