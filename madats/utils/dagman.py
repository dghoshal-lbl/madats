"""
`madats.utils.dagman`
====================================

.. currentmodule:: madats.management.dagman

:platform: Unix, Mac
:synopsis: Utility module providing functions to use and manage workflow DAGs

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import collections
from collections import deque
    
"""
returns the list of predecessors of a task in the workflow DAG
"""
def predecessors(dag, task):
    pred = []
    for k in dag.keys():
        if task in dag[k]:
            pred.append(k)
    return pred


"""
returns the list of successors of a task in the workflow DAG
"""
def successors(dag, task):
    succ = []
    if task in dag:
        for v in dag[task]:
            succ.append(v)
    return succ


"""
** task-based batch execution order: a task is the high-level execution entity **
- the order is determined as if all the tasks are submitted as a batch of tasks
- dependencies to the tasks are maintained; so even if exexcuted sequentially
no task will be executed until all its predecessors have executed
- executable is a task

do a topological sort on the workflow DAG and generate the execution order
"""
def batch_execution_order(dag):        
    visited = {}
    task_order = []
    for task in dag:
        if task not in visited: 
            __dfs__(dag, task, visited, task_order)

    return task_order


"""
** bin-based execution order, where tasks are grouped into bins: a bin is the high-level execution entity **
- the order is determined as the minimal possible set of tasks that can be executed together
- dependencies to the tasks are used to create bins and each bin is only depdendent on the previous bin
- executable is a set of tasks

default way to assign tasks to bins
"""
def bin_execution_order(dag):
    bins_dict = {}
    max_bins = 0
    task_bins = []
    
    # PASS-1: assign bins to the tasks
    for task in dag:
        bin_size = __bin_bfs__(dag, task)
        if max_bins < bin_size:
            max_bins = bin_size

    # PASS-2: re-adjust task bins for just-in-time execution
    for task in dag:
        __readjust_bins__(dag, task, max_bins, bins_dict)

    for i in range(len(bins_dict)):
        task_bins.append(bins_dict[i])
        
    return task_bins


"""
DFS on a graph
"""
def __dfs__(dag, start, visited, task_order):
    visited[start] = True
    succs = successors(dag, start)
    for succ in succs:
        if succ not in visited:
            __dfs__(dag, succ, visited, task_order)
    task_order.insert(0, start)


"""
BFS on a graph, with an assigned bin for each visited vertex of the graph
"""
def __bin_bfs__(dag, start):
    bfs_order = []
    bfs_order.append(start)
    task_queue = deque(successors(dag, start))
    n_bins = start.bin
    for t in task_queue:
        if t.bin < start.bin + 1:
            t.bin = start.bin + 1
            n_bins = t.bin 
    while len(task_queue) != 0:
        t = task_queue.popleft()
        if t not in bfs_order:
            bfs_order.append(t)
            t_succs = successors(dag, t)
            for succ in t_succs:
                if succ.bin < t.bin + 1:
                    succ.bin = t.bin + 1
                    n_bins = succ.bin                    
                task_queue.append(succ)
    return (n_bins + 1)

    
"""
readjust the order of tasks by readjusting their assigned bins for ** just-in-time ** staging/execution
"""
def __readjust_bins__(dag, task, max_bins, bins_dict):
    min_bin = max_bins
    curr_bin = task.bin
    for succ in successors(dag, task):
        min_bin = min(min_bin, succ.bin)
        
    task.bin = max(curr_bin, min_bin-1)
    if task.bin in bins_dict:
        bins_dict[task.bin].append(task)
    else:
        bins_dict[task.bin] = [task]
