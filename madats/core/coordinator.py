"""
`madats.core.coordinator`
====================================

.. currentmodule:: madats.core.coordinator

:platform: Unix, Mac
:synopsis: Module that manages data and workflow through a Virtual Data Space (VDS)

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import uuid
import os
import shlex
from madats.core.vds import VirtualDataSpace, VirtualDataObject
from madats.utils.constants import ExecutionMode, Policy
from madats.management import workflow_manager, execution_manager, data_manager
import sys
from collections import namedtuple

'''
Coordinates the movement of data between multiple storage tiers through VDS (manages VDS and virtual data objects)
 - creates data tasks and a DAG -- manages `WHAT' data is moved
'''

"""
given a workflow, map it to VDS
"""
def map(workflow, language='yaml', policy=Policy.NONE):
    vds = workflow_manager.parse(workflow, language)
    vds.data_management_policy = policy
    return vds


"""
unmap a VDS into a workflow DAG
"""
def unmap(vds):
    policy = vds.data_management_policy
    if policy == Policy.WORKFLOW_AWARE:
        data_manager.dm_workflow_aware(vds)
    elif policy == Policy.STORAGE_AWARE:
        data_manager.dm_storage_aware(vds)

    dag = vds.get_task_dag()
    return dag


"""
manage VDS by managing data as per the defined policy
 - create data tasks and compute tasks
 - manage the workflow consisting of data and compute tasks
"""
def manage(vds, execute_mode=ExecutionMode.DAG):
    policy = vds.data_management_policy
    if policy == Policy.WORKFLOW_AWARE:
        data_manager.dm_workflow_aware(vds)
    elif policy == Policy.STORAGE_AWARE:
        data_manager.dm_storage_aware(vds)
        
    dag = vds.get_task_dag()
    #print("**************")
    #workflow_manager.display(dag)
    #print("**************")

    execution_manager.execute(dag, execute_mode) 


"""
validate a VDS: validate if the params of a task and the vdo consumer/producer
list match
"""
def validate(vds):
    unmapped_vdos = {}
    Result = namedtuple('Result', 'isvalid unmapped')
    for vdo in vds.vdos:
        for prod in vdo.producers:
            for param in prod.params:
                if type(param) == VirtualDataObject:
                    if not vds.vdo_exists(param.__id__):
                        unmapped_vdos[param.__id__] = param

        for cons in vdo.consumers:
            for param in cons.params:
                if type(param) == VirtualDataObject:
                    if not vds.vdo_exists(param.__id__):
                        unmapped_vdos[param.__id__] = param
        
    valid = True
    if len(unmapped_vdos) > 0:
        valid = False

    result = Result(isvalid=valid, unmapped=unmapped_vdos)
    return result


"""
query the VDS
"""
def query(vds, metrics):
    if type(metrics) != list:
        print("Metrics must be of type `list`")
        sys.exit(1)

    metric_results = vds.__query_elements__
    query_results = {}
    for metric in metrics:
        if metric in metric_results:
            query_results[metric] = metric_results[metric]
        else:
            query_results[metric] = None
    return query_results
