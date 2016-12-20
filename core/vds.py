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
        self.copyTo = []  # can be copied to multiple storage layers at a time
        self.copyFrom = None  # can be copied from only one storage layer at a time
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

A VDS consists of VDOs and provides the following operations:
VDO management operations:
  - add: adds a vdo to VDS
  - copy: copies a vdo into another vdo, copies the producers and consumers of the src vdo as well
  - delete: deletes a vdo
VDO query operations: 
  - retrieve: returns the list of vdos in vds
  - search: returns a vdo, if exists, for a data object
  
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
        
        vdo_id = self.vdo_exists(dest_id, relative_path)
        if vdo_id != None:
            return self.__vdo_dict__[vdo_id]

        vdo = VirtualDataObject(dest_id, relative_path)
        vdo_src.copyTo.append(dest_id)
        vdo.copyFrom = vdo_src.storage_id
        vdo.consumers = [cons for cons in vdo_src.consumers]
        vdo.producers = [prod for prod in vdo_src.producers]
        self.add(vdo)
        return vdo

    '''
    deletes a VDO from the VDS
    '''
    def delete(self, vdo):
        del self.__vdo_dict__[vdo.__id__]
        self.vdos.remove(vdo.__id__)

    '''
    search
    '''
    def get_vdo(self, vdo_id):
        if vdo_id in self.__vdo_dict__:
            return self.__vdo_dict__[vdo_id]
        else:
            print("VDO ({}) does not exist!".format(vdo_id))
            return None

    '''
    retrieve
    '''
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
