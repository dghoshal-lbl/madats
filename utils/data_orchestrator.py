'''
a data orchestrator simulator
'''

import sys
from db.loader import DbLoader
from db.monitor import DbMonitor
import time
import signal

def update_data_task(task_id):
    db_loader = DbLoader()
    time.sleep(5)
    db_loader.update_status(task_id, 'RUNNING')
    time.sleep(5)
    db_loader.update_status(task_id, 'COMPLETED')

def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    print('Running data orchestrator daemon ...')
    db_monitor = DbMonitor()
    while True:
        records = db_monitor.find_data_tasks()
        for rec in records:
            task_id = rec['task_id']
            update_data_task(task_id)
        time.sleep(2)


