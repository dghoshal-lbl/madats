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
import os
import shlex
from madats.core.vds import VirtualDataSpace, VirtualDataObject
from madats.utils.constants import ExecutionMode

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''

"""
maps a task into a VDS, making it a collection of VDOs
"""
def __map2vds__(vds, task, vdo_map):
    for input in task.inputs:
        data_object = os.path.abspath(input)
        if data_object not in vdo_map:
            vdo = VirtualDataObject(data_object)
            vdo_map[data_object] = vdo
        else:
            vdo = vdo_map[data_object]
        vdo.add_consumer(task)
        vds.add(vdo)

    for output in task.outputs:
        data_object = os.path.abspath(output)
        if data_object not in vdo_map:
            vdo = VirtualDataObject(data_object)
            vdo_map[data_object] = vdo
        else:
            vdo= vdo_map[data_object]
        vdo.add_producer(task)
        vds.add(vdo)


"""
given a workflow, map it to VDS
"""
def create(workflow, language='yaml'):
    if language == 'yaml':
        task_list, input_map = workflow_manager.parse_yaml(workflow)
    else:
        print('Invalid workflow description language {}'.format(language))
        sys.exit()
    vdo_map = {}
    vds = VirtualDataSpace()
    for task in task_list:
        __map2vds__(vds, task, vdo_map)
    
    return vds


"""
manage a VDS using different data management strategies
creates a DAG of the extended workflow containing data tasks and compute tasks
- it's an adjacency list representation of the graph where the list pertaining 
to a vertex contains the vertices you can reach directly from that vertex
"""
# not required to be present here, move it to management.data_manager
#def plan(vds, policy):
    

        
"""
manage VDS by managing data and executing workflow
"""
def manage(vds, execute_mode=ExecutionMode.DAG, scheduler=None):
    dag = {}
    vdo_list = vds.get_vdo_list()
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
    
    execution_manager.execute(dag, execute_mode) 


"""
query the VDS
"""
def query(vds, query):
    print('Query interface is not yet implemented!')
    pass


"""
destroy the VDS
"""
def destroy(vds):
    vdos = vds.get_vdo_list()
    for vdo in vdos:
        vds.delete(vdo)

