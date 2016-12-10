"""
module to create a Virtual Data Space
"""

# make this a generic interface to multiple file systems
#import storage_layer
import os

__VDS_MOUNTPT__ = '/vds'

'''
creates and operates on a virtual data object (VDO)
A VDO can exist across multiple VDS's
'''
class VirtualDataObject():
    def __init__(self, storage_id, relative_path):
        self.storage_id = storage_id
        self.relative_path = relative_path # relative to storage mount point
        self.vds_path = os.path.join(__VDS_MOUNTPT__, relative_path)
        self.__id__ = self.storage_id + ':' + self.vds_path
        #self.vdo_name = self.__create_vdo__()
        #self.vdo_name = vds.get_mapped_vdo_name(storage_id, data_path)
        self.copiedTo = []  # can be copied to multiple storage layers at a time
        self.copiedFrom = None  # can be copied from only one storage layer at a time
        self.producers = []
        self.consumers = []
        self.persist = True
        #vds.add(self)
        
    '''
    def __create_vdo__(self):
        mount_pt = storage_layer.get_fs_map(self.storage_id)
        vds_path = self.fs_object.replace(mount_pt, __VDS_MOUNTPT__)
        return (self.storage_id + ':' + vds_path)
    '''

    def add_producer(self, task):
        self.producer.append(task)

    def add_consumer(self, task):
        self.consumers.append(task)

#    def persist(self):
#        self.persist = True
 
######################################################################################

'''
creates a virtual data space (VDS) connecting the virtual data objects through
other virtual data objects

A VDS consists of VDOs and allows three operations on VDOs: add, copy, delete
'''
class VirtualDataSpace():
    def __init__(self):
        self.__vdo_dict__ = {}
        self.vdos = []

    '''
    adds a VDO to the VDS
    '''
    def add(self, vdo):
        self.__vdo_dict__[vdo.__id__] = vdo
        self.vdos.append(vdo.__id__)

    '''
    copies a VDO to another VDO in the VDS
    '''
    def copy(self, vdo_src, dest_id):
        relative_path = vdo_src.relative_path
        vdo = VirtualDataObject(dest_id, relative_path)
        vdo_src.copiedTo.append(dest_id)
        vdo.copiedFrom = vdo_src.storage_id
        return vdo

    '''
    deletes a VDO from the VDS
    '''
    def delete(self, vdo):
        del self.__vdo_dict__[vdo.__id__]
        self.vdos.remove(vdo.__id__)

    def get_vdo(self, vdo_id):
        return self.__vdo_dict__[vdo_id]

    def get_vdo_list(self):
        vdo_list = [self.__vdo_dict__[id] for id in self.__vdo_dict__]
        return vdo_list

    def vdo_exists(self, storage_id, relative_path):
        vds_path = os.path.join(__VDS_MOUNTPT__, relative_path)
        vdo_id = storage_id + ':' + vds_path
        if vdo_id in self.__vdo_dict__:
            return vdo_id
        else:
            return None
