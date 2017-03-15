'''
Data management abstraction
'''

import abc
import os
from plugins import plugin_loader
from core.task import DataTask

class DataAbstract():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Data Management Abstraction')
        self.__data_tasks__ = {}
        self.__storage_plugin__ = plugin_loader.load_storage_plugin()
        self.__storage_hierarchy__ = self.__storage_plugin__.get_hierarchy()

    def __get_abspath__(self, vdo):
        mount_pt = self.__storage_plugin__.get_storage_path(self.__storage_hierarchy__, vdo.storage_id)
        relative_path = vdo.relative_path
        abspath = os.path.join(mount_pt, relative_path)

        return abspath
        

    '''
    creates a data task to move a virtual data object to a different storage layer(s)
    '''
    def create_data_task(self, vds, vdo_src, vdo_dest, **kwargs):
        src_data = self.__get_abspath__(vdo_src)
        dest_data = self.__get_abspath__(vdo_dest)
        # if staging in data
        if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
            dt = vdo_src.__id__ + vdo_dest.__id__
            if dt in self.__data_tasks__:
                print('Data task ({}) already exists'.format(dt))
                return
            else:
                src = vdo_src.storage_id + vdo_src.relative_path 
                dest = vdo_dest.storage_id + vdo_dest.relative_path 
                print('Data task ({} -> {}) created'.format(src, dest))

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

            data_task = DataTask(src_data, dest_data, **kwargs)
            self.__data_tasks__[dt] = data_task
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
        # if staging out data
        elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0) or \
                (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
            #dt = vdo_dest.__id__ + '=>' + vdo_src.__id__
            dt = vdo_src.__id__ + vdo_dest.__id__
            if dt in self.__data_tasks__:
                print('Data task ({}) already exists'.format(dt))
                return
            else:
                src = vdo_src.storage_id + vdo_src.relative_path 
                dest = vdo_dest.storage_id + vdo_dest.relative_path 
                print('Data task ({} -> {}) created'.format(src, dest))
                
            """
            update the I/O paramters to use the moved data
            """
            for task in vdo_src.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == dest_data:
                        task.params[i] = src_data
            for task in vdo_src.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == dest_data:
                        task.params[i] = src_data            

            """
            create a data task and add it to the respective VDOs
            """
            data_task = DataTask(dest_data, src_data, **kwargs)
            self.__data_tasks__[dt] = data_task

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


    @abc.abstractmethod
    def datamgmt_strategy(self, vds, **kwargs):
        #print('Manage VDS by defining data management strategies')
        raise NotImplementedError()
