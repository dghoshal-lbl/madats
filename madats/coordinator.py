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
from plugins import plugin_loader
from madats.core.vds import VirtualDataSpace, VirtualDataObject
from madats.core.task import Task, DataTask

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''

    
'''
creates a virtual data object (vdo) for a file system data object
'''
def __create_vdo__(data_object, storage_plugin, storage_hierarchy):
    
    storage_id, relative_path = storage_plugin.get_id_path(storage_hierarchy, data_object)
    
    if storage_id and relative_path:
        vdo = VirtualDataObject(storage_id, relative_path)
        return vdo
    else:
        print('Invalid data object')
        return None


'''
maps a task into a VDS, making it a collection of VDOs
'''
def __map2vds__(vds, task, data_vdo_map, storage_plugin, storage_hierarchy):
    for input in task.inputs:
        data_object = os.path.abspath(input)
        if data_object not in data_vdo_map:
            vdo = __create_vdo__(data_object, storage_plugin, storage_hierarchy)
            data_vdo_map[data_object] = vdo
        else:
            vdo = data_vdo_map[data_object]
        vdo.add_consumer(task)
        vds.add(vdo)

    for output in task.outputs:
        data_object = os.path.abspath(output)
        if data_object not in data_vdo_map:
            vdo = __create_vdo__(data_object, storage_plugin, storage_hierarchy)
            data_vdo_map[data_object] = vdo
        else:
            vdo= data_vdo_map[data_object]
        vdo.add_producer(task)
        vds.add(vdo)


'''
given a workflow, map it to VDS
'''
def create(workflow):
    storage_plugin = plugin_loader.load_storage_plugin()
    storage_hierarchy = storage_plugin.get_hierarchy()

    workflow_plugin = plugin_loader.load_workflow_plugin()
    tasks = workflow_plugin.parse(workflow)
    data_vdo_map = {}
    vds = VirtualDataSpace()
    for task in tasks:
        __map2vds__(vds, task, data_vdo_map, storage_plugin, storage_hierarchy)
    
    return vds


'''
manage a VDS using different data management strategies
creates a DAG of the extended workflow containing data tasks and compute tasks
- it's an adjacency list representation of the graph where the list pertaining 
to a vertex contains the vertices you can reach directly from that vertex
'''
def plan(vds, policy, **kwargs):
    datamgr_plugin = plugin_loader.load_datamgr_plugin()
    status = datamgr_plugin.policy_engine(vds, policy, **kwargs)
    
    return status


'''
manage VDS by managing data and executing workflow
'''
def manage(vds, async_mode=False, scheduler=None, **kwargs):
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

    if scheduler != None:
        scheduling_plugin = plugin_loader.load_scheduling_plugin()
        scheduling_plugin.set(scheduler, **kwargs)
        submit_id = scheduling_plugin.submit(dag, async_mode)
        # if true that means all jobs are submitted together with dependencies
        if async_mode == True:
            scheduling_plugin.wait(submit_id)
        status = scheduling_plugin.status(submit_id)
    else:
        execution_plugin = plugin_loader.load_execution_plugin()
        exec_id = execution_plugin.execute(dag, async_mode, **kwargs)
        if async_mode == True:
            execution_plugin.wait(exec_id)
        status = execution_plugin.status(exec_id)
    return status
     

'''
query the VDS
'''
def query(vds, query):
    print('Query interface is not yet implemented!')
    pass


'''
destroy the VDS
'''
def destroy(vds):
    vdos = vds.get_vdo_list()
    for vdo in vdos:
        vds.delete(vdo)

