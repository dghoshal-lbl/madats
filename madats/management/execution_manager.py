"""
`madats.management.execution_manager`
====================================

.. currentmodule:: madats.management.execution_manager

:platform: Unix, Mac
:synopsis: Module that manages the execution of workflow tasks

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

from madats.utils import dagman
from madats.core.scheduler import Scheduler
from madats.core.task import Task
import time
import os
import threading
try:
    import Queue as queue
except:
    import queue
import subprocess
import uuid

result_list = []
taskmap = {}
lock = threading.Lock()

_workflow_id = str(uuid.uuid4())
outdir = os.path.expandvars('$MADATS_HOME/outdir')

"""
workers processing the workflow tasks/stages
"""
def worker(taskq):
    while True:
        try:
            task = taskq.get()
            job_script = generate_script(task)
            '''
            wait until all the dependencies of the task have finished
            '''
            lock.acquire()
            dep_remaining = taskmap[task.__id__]
            lock.release()
            while dep_remaining != 0:
                time.sleep(0.001)
                lock.acquire()
                dep_remaining = taskmap[task.__id__]
                lock.release()

            #print("{} {}".format(task.__id__, taskmap))
            '''
            submit the task
            '''
            print("[Workflow-{}] Submitted task-{}".format(_workflow_id, task.__id__))
            result = submit(job_script, task.scheduler)
            print("[Workflow-{}] Finished task-{}".format(_workflow_id, task.__id__))
            
            '''
            once the task completes, notify the dependents
            '''
            lock.acquire()
            for succ in task.successors:
                taskmap[succ.__id__] -= 1
            result_list.append(result)
            #print("Result: {}".format(result))
            lock.release()
            taskq.task_done()
        except queue.Empty:
            return


"""
submit a script for execution
"""
def submit(script, scheduler):
    submit_command = "{} {}".format(Scheduler.submit_command(scheduler), script)
    try:
        output = subprocess.check_output([submit_command], shell=True)
        if scheduler != Scheduler.NONE:
            output = monitor(output, scheduler)

        return output
    except Exception as e:
        print("Job submission error:")
        print(e)


"""
monitor a batch job script
"""
def monitor(job_id, scheduler):
    query_command = "{} | grep {} | awk '{{print $1}}'".format(Scheduler.status_command(scheduler), job_id)

    try:
        result = subprocess.check_output([submit_command], shell=True)
        while result != '':
            time.sleep(1)
            result = subprocess.check_output([submit_command], shell=True)
    except Exception as e:
        print("Job monitoring error:")
        print(e)
    

"""
Execute a workflow DAG: generate individual task scripts and then run
- every task can be in one of the three states: WAIT, RUN, COMPLETE
- when all the predecessors of a task have finished, it goes from WAIT to RUN
- a task waits on the state to be changed
"""
def execute(dag, **kwargs):
    global _script_dir
    _script_dir = os.path.join(outdir, _workflow_id)
    if not os.path.exists(_script_dir):
        os.makedirs(_script_dir)

    print("[Workflow-{}] Getting tasks".format(_workflow_id))
    execution_order = dagman.batch_execution_order(dag)
    taskq = queue.Queue()

    print("[Workflow-{}] Executing tasks".format(_workflow_id))
    for task in execution_order:
        taskq.put(task)
        taskmap[task.__id__] = len(task.predecessors)
        
    #print(taskmap)

    for i in range(len(execution_order)):
        thread = threading.Thread(target=worker, args=(taskq,))
        thread.daemon = True
        thread.start()

    taskq.join()
    print("[Workflow-{}] Finished execution".format(_workflow_id))
    print("{}".format(result_list))


"""
generates the submission and execution script for a task
"""
def generate_script(task):
    command = task.command
    param_list = [str(p) for p in task.params]
    params = " ".join(param_list)    
    script_name = task.__id__ + '.sub'
    script = os.path.join(_script_dir, script_name)
    with open(script, 'w') as f:        
        f.write("#!/bin/bash\n")
        if task.scheduler != Scheduler.NONE:
            for opt in task.scheduler_opts:
                directive = Scheduler.get_directive(task.scheduler, opt)
                if directive != None:
                    value = str(task.scheduler_opts[opt])
                    directive_stmt = directive + '=' + value + '\n'
                    f.write(directive_stmt)
        f.write(command + ' ' + params + '\n')

    os.chmod(script, 0o744)
    
    #return os.path.abspath(script)
    return script


'''
test main
'''
if __name__ == '__main__':
    task1 = Task()
    task1.command = 'echo'
    task1.params = ['hello']

    task2 = Task()
    task2.command = 'echo'
    task2.params = ['world1']

    task3 = Task()
    task3.command = 'echo'
    task3.params = ['world2']

    task4 = Task()
    task4.command = 'echo'
    task4.params = ['world1-world2']

    task1.add_successor(task2)
    task1.add_successor(task3)
    task2.add_predecessor(task1)
    task3.add_predecessor(task1)
    task4.add_predecessor(task2)
    task4.add_predecessor(task3)
    task2.add_successor(task4)
    task3.add_successor(task4)

    dag = {}
    #dag[task2] = [task1]
    #dag[task3] = [task1]
    #dag[task4] = [task2, task3]

    dag[task1] = [task2, task3]
    dag[task2] = [task4]
    dag[task3] = [task4]

    execute(dag)

    

        
    