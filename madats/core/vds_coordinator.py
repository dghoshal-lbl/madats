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
from core.vds import VirtualDataSpace, VirtualDataObject
from core.task import Task, DataTask

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''
class Coordinator():
    def __init__(self):
        self.__data_vdo_map__ = {}
        self.__storage_plugin__ = plugin_loader.load_storage_plugin()
        self.__storage_hierarchy__ = self.__storage_plugin__.get_hierarchy()


    '''
    creates a virtual data object (vdo) for a file system data object
    '''
    def __create_vdo__(self, data):
        data_object = os.path.abspath(data)
        if data_object in self.__data_vdo_map__:
            return self.__data_vdo_map__[data_object]

        storage_id, relative_path = self.__storage_plugin__.get_id_path(self.__storage_hierarchy__, data_object)

        if storage_id and relative_path:
            vdo = VirtualDataObject(storage_id, relative_path)
            self.__data_vdo_map__[data_object] = vdo
            return vdo
        else:
            print('Invalid data object')
        return None

    '''
    translates a data object parameter into a VDO parameter
    '''
    '''
    def __map_params__(vds, task_args):
        args = shlex.split(task_args)
        params = []
        for arg in args:
            if arg in self.__data_vdo_map__:
                params.append(self.__data_vdo_map__[arg])
            else:
                params.append(arg)
        return params
    '''

    '''
    maps a task into a VDS, making it a collection of VDOs
    '''
    def __map2vds__(self, vds, task):
        for input in task.inputs:
            vdo = self.__create_vdo__(input)
            vdo.add_consumer(task)
            vds.add(vdo)

        for output in task.outputs:
            vdo = self.__create_vdo__(output)
            vdo.add_producer(task)
            vds.add(vdo)

        #task.params = self.__map_params__(vds, task.args)

    # entry point is the execution manager that uses the workflow plugin to parse a workflow into an intermediate vds_workflow representation
    '''
    given a workflow, map it to VDS
    '''
    def create_vds(self, workflow):
        workflow_plugin = plugin_loader.load_workflow_plugin()
        tasks = workflow_plugin.parse(workflow)
        vds = VirtualDataSpace()
        for task in tasks:
            self.__map2vds__(vds, task)

        return vds


    '''
    manage a VDS using different data management strategies
    creates a DAG of the extended workflow containing data tasks and compute tasks
    - it's an adjacency list representation of the graph where the list pertaining 
      to a vertex contains the vertices you can reach directly from that vertex

    '''
    def manage_vds(self, vds, **kwargs):
        datamgr_plugin = plugin_loader.load_datamgr_plugin()
        datamgr_plugin.extend(vds, **kwargs)
        
        extended_dag = {}
        vdo_list = vds.get_vdo_list()
        for vdo in vdo_list:
            for prod in vdo.producers:
                if prod not in extended_dag:
                    extended_dag[prod] = []
                for cons in vdo.consumers:
                    if cons not in extended_dag[prod]:
                        extended_dag[prod].append(cons)
            for con in vdo.consumers:
                if con not in extended_dag:
                     extended_dag[con] = []

        return extended_dag

    '''
    query the VDS
    '''
    def query_vds(self, vds, query):
        pass


    '''
    destroy the VDS
    '''
    def destroy_vds(self, vds):
        vdos = vds.get_vdo_list()
        for vdo in vdos:
            vds.delete(vdo)

