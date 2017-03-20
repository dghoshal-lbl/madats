from abstractions.system_interfaces import AbstractStorage
import sys

class MadatsStorage(AbstractStorage):

    def get_hierarchy(self):
        '''
        TODO read properties file containing the storage hierarchy properties
        '''
        storage_hierarchy = {'archive': '/archive/', 'scratch': '/scratch/', 'burst': '/dw/', 'css': '/css/'}
        return storage_hierarchy

    def get_id_path(self, storage_hierarchy, data_object):
        if storage_hierarchy == None:
            print('Storage hierarchy is not set!')
            sys.exit(1)
        for k in storage_hierarchy:
            if storage_hierarchy[k] in data_object:
                relative_path = data_object.replace(storage_hierarchy[k], '')
                return k, relative_path
        print('Unknown storage path: no storage identifier found for the data object!')
        return None, None

    def get_storage_path(self, storage_hierarchy, storage_id):
        if storage_id in storage_hierarchy:
            return storage_hierarchy[storage_id]
        else:
            print('Unknown storage identifier!')
            return None

    def get_storage_id(self, storage_hierarchy, mount_point):
        for k in storage_hierarchy:
            if storage_hierarchy[k] == mount_point:
                return k
        print('Unknown storage path: no storage identifier found!')
        return None
    
if __name__ == '__main__':
    storage_manager = MadatsStorageManager()    
    hier = storage_manager.get_hierarchy()
    id, path = storage_manager.get_id_path(hier, '/dw/foo.txt')
    print(id, path)
