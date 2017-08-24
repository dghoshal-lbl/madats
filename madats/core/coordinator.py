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
from madats.utils.constants import ExecutionMode, Policy
from madats.management import workflow_manager, execution_manager, data_manager

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''

"""
maps a task into a VDS, making it a collection of VDOs
"""
def __map2vds__(vds, task):
    for input in task.inputs:
        data_object = os.path.abspath(input)
        vdo = vds.create_vdo(data_object)
        vdo.add_consumer(task)

    for output in task.outputs:
        data_object = os.path.abspath(output)
        vdo = vds.create_vdo(data_object)
        vdo.add_producer(task)



"""
initialize a VDS
"""
def initVDS():
    vds = VirtualDataSpace()
    return vds


"""
given a workflow, map it to VDS
"""
def map(workflow, language='yaml'):
    if language == 'yaml':
        task_list, input_map = workflow_manager.parse_yaml(workflow)
    else:
        print('Invalid workflow description language {}'.format(language))
        sys.exit()
    vds = VirtualDataSpace()
    for task in task_list:
        __map2vds__(vds, task)
    
    '''
    for vdo in vds.vdos:
        prods = [prod.params for prod in vdo.producers]
        conss = [cons.params for cons in vdo.consumers]
        print("{} {} {}".format(vdo.abspath, prods, conss))
    '''
    return vds


"""
manage a VDS using different data management strategies
creates a DAG of the extended workflow containing data tasks and compute tasks
- it's an adjacency list representation of the graph where the list pertaining 
to a vertex contains the vertices you can reach directly from that vertex
"""
def plan(vds):
    policy = vds.data_management_policy
    if policy == Policy.NONE:
        return
    elif policy == Policy.WORKFLOW_AWARE:
        data_manager.dm_workflow_aware(vds)
    elif policy == Policy.STORAGE_AWARE:
        data_manager.dm_storage_aware(vds)
    
        
"""
manage VDS by managing data and executing workflow
"""
def manage(vds, execute_mode=ExecutionMode.DAG):
    dag = {}
    for vdo in vds.vdos:
        for prod in vdo.producers:
            if prod not in dag:
                dag[prod] = []
            for cons in vdo.consumers:
                '''
                - add the dependencies for each task
                - avoid self-dependencies to avoid deadloack  
                '''
                if cons not in dag[prod] and cons != prod:
                    dag[prod].append(cons)
                    cons.add_predecessor(prod)
                    prod.add_successor(cons)

        for con in vdo.consumers:
            if con not in dag:
                dag[con] = []
    
                
    print("**************")
    workflow_manager.display(dag)
    print("**************")

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
    vdos = vds.vdos
    for vdo in vdos:
        vds.delete(vdo)

