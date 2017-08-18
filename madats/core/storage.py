"""
`madats.core.storage`
====================================

.. currentmodule:: madats.core.storage

:platform: Unix, Mac
:synopsis: Module abstracting the storage hierarchy consisting of multiple tiers

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import hashlib
import os
import yaml
import sys

class StorageHierarchy(object):
    def __init__(self):
        storage_config = os.path.expandvars('$MADATS_HOME/config/storage.yaml')
        print('Reading storage config: {}'.format(storage_config))                
        self._hierarchy = self.parse(storage_config)
        self._mount_points = {}
        for k,v in self._hierarchy.items():
            self._mount_points[v['mount']] = str(k)

    def parse(self, storage_config):
        with open(storage_config, 'r') as config:
            storage_yaml = yaml.load(config)
            if 'system' in storage_yaml:
                system_name = storage_yaml['system']
                if system_name in storage_yaml:
                    return self.__get_storage_hierarchy__(storage_yaml[system_name])
                else:
                    print('No storage configuration specified for system {}'.format(system_name))
                    sys.exit()
            else:
                print('No system specified for storage configuration')
                sys.exit()
                

    def __get_storage_hierarchy__(self, storage_yaml):
        hierarchy = {}
        for tier in storage_yaml:
            hierarchy[tier] = {}
            tier_info = storage_yaml[tier]
            if 'mount' in tier_info:
                hierarchy[tier]['mount'] = tier_info['mount']
            if 'persist' in tier_info:
                hierarchy[tier]['persist'] = tier_info['persist']
            if 'interface' in tier_info:
                hierarchy[tier]['interface'] = tier_info['interface']
            if 'bandwidth' in tier_info:
                hierarchy[tier]['bandwidth'] = tier_info['bandwidth']

        return hierarchy
            
    def get_mount_point(self, storage_id):
        if storage_id not in self._hierarchy:
            print('Storage-id ({}) is not defined!'.format(storage_id))
            return None
        else:
            return self._hierarchy[storage_id]['mount']
    
    def get_storage_id(self, datapath):
        path = os.path.abspath(datapath)
        while not os.path.ismount(path):
            path = os.path.dirname(path)
            '''
            if the mount point is not found by the system,
            but is present in storage.yaml configuration
            that lists the storage tiers
            '''
            if path in self._mount_points:
                return self._mount_points[path]
        
        '''
        if the mount point is not present in the storage.yaml configuration,
        assign defaults
        '''
        print('Storage-id not set for mount point {}'.format(path))
        default_id = path.replace('/', '_')
        if default_id == '_':
            default_id = 'root'                

        # this should be Log.INFO, once logging framework is implemented
        print('Defaulting storage-id for the mount point to {}'.format(default_id))
        '''
        if the path resolves to the root directory, then the mount-point
        possibly doesn't exist on the system yet; hence, assign 'root'
        '''
        self._mount_points[path] = default_id
        self._hierarchy[default_id] = {'mount': path}

        return default_id
            

__storage_hierarchy__ = StorageHierarchy()

"""
return a decoded hash of a datapath
"""
def get_data_id(datapath):
    md5 = hashlib.md5()
    md5.update(datapath)
    return md5.hexdigest()

"""
return the storage identifier and relative path w.r.t. the storage mount point
"""
def get_elements(datapath):
    storage_id = __storage_hierarchy__.get_storage_id(datapath)
    mount_point = __storage_hierarchy__.get_mount_point(storage_id)

    if mount_point == '/':
        relative_path = datapath.replace(mount_point, '', 1)
    else:
        relative_path = datapath.replace(mount_point, '')

    if relative_path[0] == '/':
        relative_path = relative_path[1:]

    return storage_id, relative_path 
        

"""
get a new datapath on a different storage layer
"""
def build_data_path(dest_id, relative_path):
    mount_pt = __storage_hierarchy__.get_mount_point(dest_id)
    datapath = os.path.join(mount_pt, relative_path)
    return datapath

"""
get the mount point from a datapath
"""
def get_data_mount_point(datapath):
    path = os.path.abspath(datapath)
    while not os.path.ismount(path):
        path = os.path.dirname(path)

    if path == '/':
        return None
    else:
        return path


if __name__ == '__main__':
    d1 = '/home/dghoshal/dir1'
    d2 = '/Volumes/MobileBackups/Backups.backupdb'
    d3 = '/scratch/scratchdirs/cscratch1/dghoshal'
    print("{}: {}".format(d1, get_elements(d1)))
    print("{}: {}".format(d2, get_elements(d2)))
    print("{}: {}".format(d3, get_elements(d3)))