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
from madats.vds import VirtualDataSpace, VirtualDataObject
from madats.vds import Task, DataTask

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''

    
'''
creates a virtual data object (vdo) for a file system data object
'''
def __create_vdo__(data_object, dataprop):
    vdo = VirtualDataObject(data_object)
    if 'size' in dataprop:
        vdo.set_size(dataprop['size'])
    if 'persist' in dataprop:
        vdo.persist(dataprop['persist'])
    if 'replicate' in dataprop:
        vdo.replicate(dataprop['replicate'])
        
    return vdo


'''
maps a task into a VDS, making it a collection of VDOs
'''
def __map2vds__(vds, task, data_vdo_map, data_properties):
    for input in task.inputs:
        #data_object = os.path.abspath(input)
        data_object = input
        if data_object not in data_vdo_map:
            dataprop = {}
            if data_object in data_properties:
                dataprop = data_properties[data_object]
            vdo = __create_vdo__(data_object, dataprop)
            data_vdo_map[data_object] = vdo
        else:
            vdo = data_vdo_map[data_object]
        vdo.add_consumer(task)
        vds.add(vdo)

    for output in task.outputs:
        #data_object = os.path.abspath(output)
        data_object = output
        if data_object not in data_vdo_map:
            dataprop = {}
            if data_object in data_properties:
                dataprop = data_properties[data_object]
            vdo = __create_vdo__(data_object, dataprop)
            data_vdo_map[data_object] = vdo
        else:
            vdo= data_vdo_map[data_object]
        vdo.add_producer(task)
        vds.add(vdo)


'''
creates a virtual data space (VDS) in MaDaTS
'''
def create():
    vds = VirtualDataSpace()
    return vds

'''
given a workflow, map it to VDS
'''
def map(vds, workflow, data_properties={}):
    workflow_plugin = plugin_loader.load_workflow_plugin()
    #workflow_plugin = plugins.workflow_plugin
    tasks = workflow_plugin.parse(workflow)
    data_vdo_map = {}
    for task in tasks:
        __map2vds__(vds, task, data_vdo_map, data_properties)

'''
manage a VDS using different data management strategies
creates a DAG of the extended workflow containing data tasks and compute tasks
- it's an adjacency list representation of the graph where the list pertaining 
to a vertex contains the vertices you can reach directly from that vertex
'''
def plan(vds, policy, **kwargs):
    #plugins = Plugins()
    #datamgr_plugin = plugins.datamgr_plugin
    datamgr_plugin = plugin_loader.load_datamgr_plugin()
    status = datamgr_plugin.policy_engine(vds, policy, **kwargs)
    
    return status


'''
manage VDS by managing data and executing workflow
'''
def manage(vds, async=False, scheduler=None, auto_exec=True, **kwargs):
    dag = {}
    vdo_list = vds.get_vdo_list()
    #plugins = Plugins()
    for vdo in vdo_list:
        for prod in vdo.producers:
            if prod not in dag:
                dag[prod] = []
            for cons in vdo.consumers:
                if cons not in dag[prod]:
                    dag[prod].append(cons)
                    prod.add_successor(cons)
                    cons.add_predecessor(prod)
        for con in vdo.consumers:
            if con not in dag:
                dag[con] = []

    status = 0
    if scheduler != None:
        #scheduling_plugin = plugins.scheduling_plugin
        scheduling_plugin = plugin_loader.load_scheduling_plugin()
        scheduling_plugin.set(scheduler, **kwargs)
        submit_ids = scheduling_plugin.submit(dag, async, auto_exec)
        if auto_exec == True:
            # if async is true that means all jobs are submitted together with dependencies
            if async == True:
                scheduling_plugin.wait(submit_ids)
            status = scheduling_plugin.status(submit_ids)
    else:
        #execution_plugin = plugins.execution_plugin
        execution_plugin = plugin_loader.load_execution_plugin()
        exec_id = execution_plugin.execute(dag, async, auto_exec, **kwargs)
        if auto_exec == True:
            if async == True:
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

