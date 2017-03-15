'''
Storage management abstraction for application programmers and users to integrate with different storage systems
'''

import abc

class StorageAbstract():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Storage management abstraction')
        #self.storage_hierarchy = None
        pass

    @abc.abstractmethod
    def get_hierarchy(self):
        #print('Query the memory/storage system to obtain memory/storage tiers')
        raise NotImplementedError()

    @abc.abstractmethod
    def get_id_path(self, storage_hierarchy, data_object):
        #print('Return storage-system identifier and relative path w.r.t. the mount point of the storage system')
        raise NotImplementedError()
        

    @abc.abstractmethod
    def get_storage_path(self, storage_hierarchy, storage_id):
        #print('Mount point of the storage-system identifier')
        raise NotImplementedError()


    @abc.abstractmethod
    def get_storage_id(self, storage_hierarchy, mount_point):
        #print('Return storage-system identifier corresponding to a mount-point')
        raise NotImplementedError()
