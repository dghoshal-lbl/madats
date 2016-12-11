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
import storage_layer
from vds import VirtualDataSpace, VirtualDataObject
from task import Task, DataTask

'''
uses data management strategies, creates data tasks and a DAG
-- manages `WHAT' data is moved
'''
class DataManagement():
    def __init__(self, vds, task_map):
        self.vds = vds
        self.task_map = task_map
        self.data_tasks = {}

    '''
    creates a virtual data object (vdo) for a file system data object
    '''
    def create_vdo(self, data_object):
        storage_id, relative_path = storage_layer.get_storage_info(data_object)
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
    limitation: if the input data itself gets modified, then this doesn't work correctly
    '''
    def create_data_task(self, vdo_src, vdo_dest):
        # if staging in
        if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
            dt_id = vdo_src.__id__ + '=>' + vdo_dest.__id__

            if dt_id in self.data_tasks:
                print('Data task already exists')
                return

            data_task = DataTask(dt_id, vdo_src, vdo_dest)
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_dest.consumers = [cons for cons in vdo_src.consumers]
            vdo_src.consumers = [data_task]
            self.task_map[dt_id] = data_task
        # if staging out
        elif len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0:
            dt_id = vdo_dest.__id__ + '=>' + vdo_src.__id__

            if dt_id in self.data_tasks:
                print('Data task already exists')
                return

            data_task = DataTask(dt_id, vdo_dest, vdo_src)
            vdo_dest.consumers = [data_task]
            vdo_dest.producers = [prod for prod in vdo_src.producers]
            vdo_src.producers = [data_task]
            self.task_map[dt_id] = data_task
        else:
            vdo_dest.producers = [prod for prod in vdo_src.producers]
            vdo_dest.consumers = [cons for cons in vdo_src.consumers]
            self.vds.delete(vdo_src)
    
    '''
    creates a DAG of the extended workflow containing data tasks and compute tasks
    '''
    def create_dag(self):
        dag = {}
        for vdo_name in self.vds.vdos:
            vdo = self.vdo_dict[vdo_name]
            for prod in vdo.producers:
                if prod.__id__ not in dag:
                    dag[prod.__id__] = []
                for cons in vdo.consumers:
                    if cons.__id__ not in dag[prod.__id__]:
                        dag[prod.__id__].append(cons.__id__)

        return dag
    
######################################################################################

'''
manages the DAG of a workflow containing compute and data tasks
-- manages `WHEN' to move the data
'''
class DAGManagement():
    def __init__(self, vds, dag, task_map):
        self.vds = vds
        self.dag = dag
        self.task_map = task_map
        self.task_order = []
        self.task_bins = []

    '''
    returns the list of predecessors of a task in the workflow DAG
    '''
    def predecessors(self, task):
        pred = []
        for k in self.dag.keys():
            if task.__id__ in self.dag[k]:
                pred.append(self.task_map[k])
        return pred


    '''
    returns the list of successors of a task in the workflow DAG
    '''
    def successors(self, task):
        succ = []
        for v in self.dag[task.__id__]:
            succ.append(self.task_map[v])
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
        start_vertices = []
        # find the root nodes
        for k in self.dag:
            task = self.task_map[k]
            preds = self.predecessors(task)
            if len(preds) == 0:
                start_vertices.append(k)

        # do a DFS on the graph
        for v in start_vertices:
            self.__dfs__(v)

        #return task_order


    '''
    ** bin-based execution order, where tasks are grouped into bins: a bin is the high-level execution entity **
    - the order is determined as the minimal possible set of tasks that can be executed together
    - dependencies to the tasks are used to create bins and each bin is only depdendent on the previous bin
    - executable is a set of tasks

    default way to assign tasks to bins
    '''
    def bin_execution_order(self):
        bins_dict = collections.OrderedDict()
        max_bins = 0

        # PASS-1: assign bins to the tasks
        for k in self.dag:
            bin_size = self.__bin_bfs__(k)
            if max_bins < bin_size:
                max_bins = bin_size

        # PASS-2: re-adjust task bins for just-in-time execution
        for k in self.dag:
            task = self.task_map[k]
            max_bins = self.__readjust_bins__(task, max_bins, bins_dict)

        for k, v in bins_dict:
            self.task_bins.append(v)

        #return task_bins

    '''
    DFS on a graph
    '''
    def __dfs__(self, start_id):        
        self.task_order.append(start_id)
        k = self.dag[start_id]
        task = self.task_map[k]
        task_stack = self.successors(task)
        while len(task_stack) != 0:
            t = task_stack.pop()
            if t.__id__ not in self.task_order:
                self.task_order.append(t.__id__)
                t_succs = self.successors(t)
                task_stack.extend(t_succs)


    '''
    BFS on a graph, with an assigned bin for each visited vertex of the graph
    '''
    def __bin_bfs__(self, start_id):
        bfs_order = []
        bfs_order.append(start_id)
        k = self.dag[start_id]
        task = self.task_map[k]        
        task_queue = deque(self.successors(task))
        n_bins = task.bin
        for t in task_queue:
            if t.bin < task.bin + 1:
                t.bin = task.bin + 1
                n_bins = t.bin 
        while len(task_queue) != 0:
            t = task_queue.popleft()
            if t.__id__ not in bfs_order:
                bfs_order.append(t.__id__)
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
        max_bin = 0
        
        return 0


    '''
    submit each task to the workload manager, and specify the task dependencies to it
    '''
    def execute_batch(self):
        self.task_order


    '''
    submit the bins to the workload manager, where each bin is dependent on its parent
    '''
    def execute_bins(self):
        self.task_bins



if __name__ == '__main__':
    vds = VirtualDataSpace()
    task_map = {}

    task1 = Task(0)
    task1.set_command('./hello_world')
    task_map[0] = task1
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    data_mgmt = DataManagement(vds, task_map)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'burst')
    data_mgmt.create_data_task(vdo1, vdo3)

    for k in task_map:
        task = task_map[k]
        print("Task ({}): {} {}".format(task.__id__, task.type, task.command))

    vdo_list = vds.get_vdo_list()
    for vdo in vdo_list:
        print("VDO: {}".format(vdo.__id__))
    #dag_mgmt = DAGManagement(vds, dag, task_map)
