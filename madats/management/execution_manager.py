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
from madats.core.vds import VirtualDataObject, Task
import time
import os
import threading
import multiprocessing
try:
    import Queue as queue
except:
    import queue
import subprocess
import uuid
from madats.utils.constants import ExecutionMode, TaskType
import shutil

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
            #print("[Workflow-{}] Submitted {} task-{}".format(_workflow_id, TaskType.name(task.type), task.__id__))
            param_list = []
            for param in task.params:
                if isinstance(param, VirtualDataObject):
                    param_list.append(param.abspath)
                else:
                    param_list.append(param)

            params = " ".join(param_list)    
            print("** Submitted: {} {}".format(task.command, params))
            result = submit(job_script, task.scheduler)
            print("** Finished: {} {}".format(task.command, params))
            #print("[Workflow-{}] Finished {} task-{}".format(_workflow_id, TaskType.name(task.type), task.__id__))
            
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
        return str(e)


"""
monitor a batch job script
"""
def monitor(job_id, scheduler):
    #query_command = "{} | grep {} | awk '{{print $1}}'".format(Scheduler.status_command(scheduler), job_id)
    #query_command = "{} | grep {} ".format(Scheduler.status_command(scheduler), job_id)
    query_command = Scheduler.status_command(scheduler)
    output = []
    try:
        result = subprocess.check_output([query_command], shell=True)
        output.append(result)
        while result.find(job_id) >= 0:
            time.sleep(1)
            result = subprocess.check_output([query_command], shell=True)
            output.append(result)
    except Exception as e:
        print("Job monitoring error:")
        print(e)
    finally:
        return '\n'.join(output)
    

"""
generates the submission and execution script for a task
"""
def generate_script(task):
    command = task.command
    #param_list = [str(p) for p in task.params]
    param_list = []
    for param in task.params:
        if isinstance(param, VirtualDataObject):
            param_list.append(param.abspath)
        else:
            param_list.append(param)

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


"""
Execute a workflow DAG: generate individual task scripts and then run
- every task can be in one of the three states: WAIT, RUN, COMPLETE
- when all the predecessors of a task have finished, it goes from WAIT to RUN
- a task waits on the state to be changed
"""
def dag_execution(dag):
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
    #shutil.rmtree(_script_dir)
    shutil.rmtree(outdir)

"""
execute a single task of the workflow
"""
def execute_task(task):
    job_script = generate_script(task)
    '''
    submit the task
    '''
    print("[Workflow-{}] Submitted task-{}".format(_workflow_id, task.__id__))
    result = submit(job_script, task.scheduler)
    print("[Workflow-{}] Finished task-{}".format(_workflow_id, task.__id__))
            
    return result


"""
Execute a workflow DAG by combining independent tasks into 'Bins'
"""
def bin_execution(dag):
    global _script_dir
    _script_dir = os.path.join(outdir, _workflow_id)
    if not os.path.exists(_script_dir):
        os.makedirs(_script_dir)

    print("[Workflow-{}] Getting tasks".format(_workflow_id))
    task_bins = dagman.bin_execution_order(dag)
    idx = 0
    for bin in task_bins:
        task_list = [t.params for t in bin]
        print('{}: {}'.format(idx, task_list))
        idx += 1
        
    result_list = []
    print("[Workflow-{}] Executing tasks".format(_workflow_id))
    for tasks in task_bins:
        num_tasks = len(tasks)
        pool = multiprocessing.Pool(processes=num_tasks)
        job_args = []
        for task in tasks:
            job_script = generate_script(task)
            scheduler = task.scheduler
            job_args.append((job_script, scheduler))

        results = [pool.apply_async(submit, args=job_arg) for job_arg in job_args]
        for result in results:
            result_list.append(result.get())
            
    print("[Workflow-{}] Finished execution".format(_workflow_id))
    print("{}".format(result_list))


"""
Execute a workflow DAG by ordering task execution based on their priorities
"""
def priority_execution(dag):
    pass


"""
Execute a workflow DAG by assigning high priority to tasks with large number of dependent jobs 
"""
def dependency_execution(dag):
    pass


"""
abstracting the workflow manager execution modes
"""
def execute(dag, mode=ExecutionMode.DAG):
    if mode == ExecutionMode.DAG:
        dag_execution(dag)
    elif mode == ExecutionMode.BIN:
        bin_execution(dag)
    elif mode == ExecutionMode.PRIORITY:
        priority_execution(dag)
    elif mode == ExecutionMode.DEPENDENCY:
        dependency_execution(dag)
    else:
        print("Workflow execution mode ({}) not implemented!".format(mode))
    