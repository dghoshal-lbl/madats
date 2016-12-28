"""
manages a virtual data space (VDS) for a workflow
- vds coordinator is always responsible for creating data tasks
  .. can be driven by user needs (UD)
  .. can be driven by storage and data properties (SD)
  .. can be driven by the workflow structure (WD)
- but managing the data tasks can be a responsibility of storage system or vds coordinator
- so, provide an API to create and connect data tasks in a workflow
- use the API to implement SD and WD, leaving scope for implementing diff algo for UD
"""
import uuid
import storage.storage_manager as storage_manager
from core.vds import VirtualDataSpace, VirtualDataObject
from core.task import Task, DataTask
import collections
from collections import deque

'''
uses data management strategies, creates data tasks and a DAG
-- manages `WHAT' data is moved
'''
class DataManagement():
    STORAGE, WORKFLOW = range(2)

    def __init__(self, vds, manager=STORAGE):
        self.vds = vds
        self.data_tasks = {}
        self.manager = manager


    def copy_command(self):
        if self.manager == self.STORAGE:
            return 'css_cp' # do it through some config/properties file                                               
        else:
            return 'cp'

    '''
    creates a virtual data object (vdo) for a file system data object
    '''
    def create_vdo(self, data_object):
        storage_id, relative_path = storage_manager.get_storage_info(data_object)
        if storage_id and relative_path:
            vdo_id = self.vds.vdo_exists(storage_id, relative_path)        
            if not vdo_id:
                vdo = VirtualDataObject(storage_id, relative_path)
                self.vds.add(vdo)
            else:
                vdo = self.vds.get_vdo(vdo_id)
            
            return vdo
        else:
            print('Invalid data object')


    '''
    creates a data task to move a virtual data object to a different storage layer(s)
    '''
    def create_data_task(self, vdo_src, vdo_dest, **kwargs):
        args = {'command': self.copy_command()}
        for k, v in kwargs.iteritems():
            args[k] = v
        # if staging in data
        if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
            dt = vdo_src.__id__ + '=>' + vdo_dest.__id__
            if dt in self.data_tasks:
                print('Data task ({}) already exists'.format(dt))
                return
            else:
                print('Data task ({}) created'.format(dt))

            data_task = DataTask(vdo_src, vdo_dest, args)
            self.data_tasks[dt] = data_task
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
            for task in vdo_dest.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest
            for task in vdo_dest.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest            
        # if staging out data
        elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0) or \
                (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
            #dt = vdo_dest.__id__ + '=>' + vdo_src.__id__
            dt = vdo_src.__id__ + '=>' + vdo_dest.__id__
            if dt in self.data_tasks:
                print('Data task ({}) already exists'.format(dt))
                return
            else:
                print('Data task ({}) created'.format(dt))

            #data_task = DataTask(vdo_dest, vdo_src, args)
            data_task = DataTask(vdo_src, vdo_dest, args)
            self.data_tasks[dt] = data_task

            vdo_dest.producers = [data_task]        
            vdo_src.consumers.append(data_task)
            vdo_dest.consumers = []
            for task in vdo_src.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_dest:
                        task.params[i] = vdo_src
            for task in vdo_src.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_dest:
                        task.params[i] = vdo_src            
        # for non-persistent intermediate data
        else:
            for task in vdo_dest.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest            
            for task in vdo_dest.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest            
            self.vds.delete(vdo_src)
            
    '''
    creates a DAG of the extended workflow containing data tasks and compute tasks
    - it's an adjacency list representation of the graph where the list pertaining 
      to a vertex contains the vertices you can reach directly from that vertex
    '''
    def create_dag(self):
        dag = {}
        vdo_list = self.vds.get_vdo_list()
        for vdo in vdo_list:
            for prod in vdo.producers:
                if prod not in dag:
                    dag[prod] = []
                for cons in vdo.consumers:
                    if cons not in dag[prod]:
                        dag[prod].append(cons)
            for con in vdo.consumers:
                if con not in dag:
                    dag[con] = []

        return dag
    
######################################################################################

'''
manages the DAG of a workflow containing compute and data tasks
-- manages `WHEN' to move the data
'''
class DAGManagement():
    def __init__(self, dag):
        self.dag = dag

    '''
    returns the list of predecessors of a task in the workflow DAG
    '''
    def predecessors(self, task):
        pred = []
        for k in self.dag.keys():
            if task in self.dag[k]:
                pred.append(k)
        return pred


    '''
    returns the list of successors of a task in the workflow DAG
    '''
    def successors(self, task):
        succ = []
        if task in self.dag:
            for v in self.dag[task]:
                succ.append(v)
        return succ


    '''
    ** task-based batch execution order: a task is the high-level execution entity **
    - the order is determined as if all the tasks are submitted as a batch of tasks
    - dependencies to the tasks are maintained; so even if exexcuted sequentially
      no task will be executed until all its predecessors have executed
    - executable is a task

    do a topological sort on the workflow DAG and generate the execution order
    '''
    def batch_execution_order(self):        
        visited = {}
        task_order = []
        for task in self.dag:
            if task not in visited: 
                self.__dfs__(task, visited, task_order)

        return task_order


    '''
    ** bin-based execution order, where tasks are grouped into bins: a bin is the high-level execution entity **
    - the order is determined as the minimal possible set of tasks that can be executed together
    - dependencies to the tasks are used to create bins and each bin is only depdendent on the previous bin
    - executable is a set of tasks

    default way to assign tasks to bins
    '''
    def bin_execution_order(self):
        bins_dict = {}
        max_bins = 0
        task_bins = []

        # PASS-1: assign bins to the tasks
        for task in self.dag:
            bin_size = self.__bin_bfs__(task)
            if max_bins < bin_size:
                max_bins = bin_size

        # PASS-2: re-adjust task bins for just-in-time execution
        for task in self.dag:
            self.__readjust_bins__(task, max_bins, bins_dict)

        for i in range(len(bins_dict)):
            task_bins.append(bins_dict[i])

        return task_bins


    '''
    DFS on a graph
    '''
    def __dfs__(self, start, visited, task_order):
        visited[start] = True
        succs = self.successors(start)
        for succ in succs:
            if succ not in visited:
                self.__dfs__(succ, visited, task_order)
        task_order.insert(0, start)


    '''
    BFS on a graph, with an assigned bin for each visited vertex of the graph
    '''
    def __bin_bfs__(self, start):
        bfs_order = []
        bfs_order.append(start)
        task_queue = deque(self.successors(start))
        n_bins = start.bin
        for t in task_queue:
            if t.bin < start.bin + 1:
                t.bin = start.bin + 1
                n_bins = t.bin 
        while len(task_queue) != 0:
            t = task_queue.popleft()
            if t not in bfs_order:
                bfs_order.append(t)
                t_succs = self.successors(t)
                for succ in t_succs:
                    if succ.bin < t.bin + 1:
                        succ.bin = t.bin + 1
                        n_bins = succ.bin                    
                    task_queue.append(succ)
        return (n_bins + 1)

    
    '''
    readjust the order of tasks by readjusting their assigned bins for ** just-in-time ** staging/execution
    '''
    def __readjust_bins__(self, task, max_bins, bins_dict):
        min_bin = max_bins
        curr_bin = task.bin
        for succ in self.successors(task):
            min_bin = min(min_bin, succ.bin)

        task.bin = max(curr_bin, min_bin-1)
        if task.bin in bins_dict:
            bins_dict[task.bin].append(task)
        else:
            bins_dict[task.bin] = [task]
