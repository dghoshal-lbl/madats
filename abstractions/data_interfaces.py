'''
Workflow abstraction: interface to translate any workflow into a collection of VDOs
'''

import abc
import os
from madats.vds import DataTask

class AbstractWorkflow():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass


    @abc.abstractmethod
    def parse(self, workflow, type=None, **kwargs):
        #print('Transform a workflow to a collection of VDOs: data as nodes, processes as edges')
        raise NotImplementedError()

#############################################################################################

'''
Data management abstraction
'''

class AbstractDatamgr():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Data Management Abstraction')
        self.__data_tasks__ = {}


    '''
    creates a data task to move a virtual data object to a different storage layer(s)
    '''
    def replace_vdo(self, vds, vdo_src, vdo_dest, **kwargs):
        src_data = vdo_src.abspath
        dest_data = vdo_dest.abspath
        #print("Replace vdo: {}[{}] {}[{}]".format(src_data, vdo_src.storage_id, dest_data, vdo_dest.storage_id))
        # if staging in data
        if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
            dt = vdo_src.__id__ + vdo_dest.__id__
            if dt in self.__data_tasks__:
                #print('Data task ({}) already exists'.format(dt))
                return

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
            self.__data_tasks__[dt] = data_task
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
        # if staging out data
        elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0):
        #or (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
            dt = vdo_src.__id__ + vdo_dest.__id__
            if dt in self.__data_tasks__:
                #print('Data task ({}) already exists'.format(dt))
                return
                
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
            self.__data_tasks__[dt] = data_task

            vdo_src.producers = [data_task]        
            vdo_dest.consumers.append(data_task)
            vdo_src.consumers = []
        # for intermediate data
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
            # if intermediate data needs to persist then create a stage out data task
            if vdo_src.persistence == True:
                dt = vdo_src.__id__ + vdo_dest.__id__
                if dt in self.__data_tasks__:
                    return
                data_task = DataTask(vdo_dest, vdo_src, **kwargs)
                self.__data_tasks__[dt] = data_task
                vdo_src.producers = [data_task]        
                vdo_dest.consumers.append(data_task)
                vdo_src.consumers = []
            else:
                vds.delete(vdo_src)


    @abc.abstractmethod
    def policy_engine(self, vds, policy, **kwargs):
        #print('Manage VDS by defining data management strategies')
        raise NotImplementedError()
