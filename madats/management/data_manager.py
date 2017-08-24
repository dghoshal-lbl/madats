"""
`madats.management.data_manager`
====================================

.. currentmodule:: madats.management.data_manager

:platform: Unix, Mac
:synopsis: Module that defines strategies for managing data across storage/file systems

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

from madats.utils.constants import Policy
from madats.core import storage
from madats.core.task import DataTask

__data_tasks__ = {}

"""
creates a data task to move a virtual data object to a different storage layer(s)
"""
def create_data_task(vds, vdo_src, vdo_dest, **kwargs):
    src_data = vdo_src.abspath
    dest_data = vdo_dest.abspath
    # if staging in data, vdo_src has no producers
    if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
        dt = vdo_src.__id__ + vdo_dest.__id__
        if dt in __data_tasks__:
            print('Data task ({}) already exists'.format(dt))
            return
        else:
            src = vdo_src.storage_id + vdo_src.relative_path 
            dest = vdo_dest.storage_id + vdo_dest.relative_path 
            print('Data stage-in task ({} -> {}) created'.format(src_data, dest_data))

        """
        update the I/O parameters if data is moved
        """
        for task in vdo_dest.consumers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data
        for task in vdo_dest.producers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data

        data_task = DataTask(vdo_src, vdo_dest, **kwargs)
        __data_tasks__[dt] = data_task
        """
        - data stagein task becomes the consumer of the original data
        - data stagein task becomes the producer of the new data
        - consumers of the original data become the consumers of the new data
        """
        vdo_dest.producers = [data_task]
        vdo_src.consumers = [data_task]
    # if staging out data, vdo_src has no consumers
    elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0) or \
            (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
        #dt = vdo_dest.__id__ + '=>' + vdo_src.__id__
        dt = vdo_src.__id__ + vdo_dest.__id__
        if dt in __data_tasks__:
            print('Data task ({}) already exists'.format(dt))
            return
        else:
            src = vdo_src.storage_id + vdo_src.relative_path 
            dest = vdo_dest.storage_id + vdo_dest.relative_path 
            print('Data stage-out task ({} -> {}) created'.format(src_data, dest_data))
                
        """
        update the I/O paramters to use the moved data
        """
        for task in vdo_src.consumers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data
        for task in vdo_src.producers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data

        """
        create a data task and add it to the respective VDOs
        """
        data_task = DataTask(vdo_dest, vdo_src, **kwargs)
        __data_tasks__[dt] = data_task

        vdo_dest.producers = [data_task]        
        vdo_src.consumers.append(data_task)
        vdo_dest.consumers = []
    # for non-persistent intermediate data
    else:
        for task in vdo_dest.consumers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data            
        for task in vdo_dest.producers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == src_data:
                    task.params[i] = dest_data            
        vds.delete(vdo_src)


def get_selected_storage(property='bandwidth'):
    storage_hierarchy = storage.get_storage_tiers()

    # assign the hierarchy order based on the storage property; in this case, the storage bandwidth
    ordered_hierarchy = {}
    sorted_values = []
    order_key = property
    max_value = 0
    fast_tier = None

    #print(storage_hierarchy)
    for tier in storage_hierarchy:
        #print("{}: {}".format(tier, storage_hierarchy[tier]))
        value = storage_hierarchy[tier][order_key]
        if value > max_value:
            max_value = value
            fast_tier = tier

    return fast_tier

"""
workflow-aware data management: data is moved only when there is an overlap
between computation and data transfer steps.
"""
def dm_workflow_aware(vds):    
    fast_tier = get_selected_storage()
    '''
    create a shallow copy of the VDO list, because new VDOs will be added to VDS now
    '''
    vdos = [v for v in vds.vdos]
    '''
    if a VDO's consumer has predecessors, or if a VDO's producer has successors,
    then the VDO can be moved and used from another storage tier
    '''
    for vdo in vdos:
        # if it's an input: create data task for staging data in
        if len(vdo.producers) == 0 and len(vdo.consumers) > 0:
            for task in vdo.consumers:
                if len(task.predecessors) > 0:
                    new_vdo = vds.copy(vdo, fast_tier)
                    create_data_task(vds, vdo, new_vdo)
                    break            
        # if it's an output, create data task for staging data out
        elif len(vdo.consumers) == 0 and len(vdo.producers) > 0:
            for task in vdo.producers:
                if len(task.successors) > 0:
                    new_vdo = vds.copy(vdo, fast_tier)
                    create_data_task(vds, vdo, new_vdo)
                    break
        # if it's intermediate data: generate/use data from fast tier
        else:
            new_vdo = vds.copy(vdo, fast_tier)
            create_data_task(vds, vdo, new_vdo)
        

"""
storage-aware data management: data is moved/kept in the fast tier for all inputs and outputs
"""
def dm_storage_aware(vds):
    fast_tier = get_selected_storage()
    '''
    create a shallow copy of the VDO list, because new VDOs will be added to VDS now
    '''
    vdos = [v for v in vds.vdos]
    for vdo in vdos:
        new_vdo = vds.copy(vdo, fast_tier)
        '''
        create a data task for each data object in VDS
        '''
        create_data_task(vds, vdo, new_vdo)

'''
def plan(vds):
    policy = vds.data_management_policy
    if policy == Policy.NONE:
        return
    elif policy == Policy.WORKFLOW_AWARE:
        dm_workflow_aware(vds)
    elif policy == Policy.STORAGE_AWARE:
        dm_storage_aware(vds)
'''
