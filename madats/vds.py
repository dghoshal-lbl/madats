"""
module to create a Virtual Data Space
"""

# make this a generic interface to multiple file systems
#import storage_layer
import os
import sys
import hashlib
import uuid
from plugins import plugin_loader

__VDS_MOUNTPT__ = '/vds'
storage_plugin = plugin_loader.load_storage_plugin()

def md5hash(storage_id, vds_path):
    vds_id = storage_id + ':' + vds_path
    md5 = hashlib.md5()
    md5.update(vds_id)
    return md5.hexdigest()

##########################################################################

class Task():
    COMPUTE = 0
    DATA = 1

    def __init__(self, command='', type=COMPUTE, **kwargs):
        self.__id__  = uuid.uuid4()
        self.command = command
        self.inputs = []
        self.outputs = []
        self.params = []
        self.partition = ''
        self.walltime = ''
        self.cpus = 0
        self.queue = ''
        self.account = ''
        self.predecessors = []
        self.successors = []
        self.bin = 0
        self.type = type
        self.kwargs = kwargs
        self.pre_exec = []
        self.post_exec = []

    def add_predecessor(self, pred):
        self.predecessors.append(pred)

    def add_successor(self, succ):
        self.successors.append(succ)

    def add_param(self, param):
        if isinstance(param, VirtualDataObject) == True:
            self.params.append(param.abspath)
        else:
            self.params.append(param)

    def get_command(self):
        return self.command

##########################################################################

class DataTask(Task):
    def __init__(self, vdo_src, vdo_dest, **kwargs):
        #print('Datatask: {} {}'.format(vdo_src.abspath, vdo_dest.abspath))
        Task.__init__(self, type=Task.DATA, **kwargs)
        self.vdo_src = vdo_src
        self.vdo_dest = vdo_dest
        #print(vdo_src.storage_id, vdo_dest.storage_id)
        #self.params = [vdo_src.abspath, vdo_dest.abspath]
        self.add_param(vdo_src)
        self.add_param(vdo_dest)
        self.command = 'madats-copy'

##########################################################################

'''
creates and operates on a virtual data object (VDO)
A VDO can exist across multiple VDS's
'''
class VirtualDataObject():
    # TODO: ideally should be data path, which should be translated into VDO
    # should have the `abspath' attribute as well
    #def __init__(self, storage_id, relative_path):
    def __init__(self, datapath):
        storage_hierarchy = storage_plugin.get_hierarchy()
        storage_id, relative_path = storage_plugin.get_id_path(storage_hierarchy, datapath)
        self.storage_id = storage_id
        self.relative_path = relative_path # relative to storage mount point        
        self.abspath = datapath
        #mount_pt = storage_plugin.get_storage_path(storage_hierarchy, storage_id)
        #self.abspath = os.path.join(mount_pt, relative_path)
        vds_path = os.path.join(__VDS_MOUNTPT__, relative_path)
        #self.__id__ = (self.storage_id, self.vds_path)
        self.__id__ = md5hash(storage_id, vds_path)
        self.copyTo = []  # can be copied to multiple storage layers at a time
        self.copyFrom = None  # can be copied from only one storage layer at a time
        self.producers = []
        self.consumers = []
        self.size = 0.0  # size in MB
        self.persistence = False
        #self.persistent_storage = []
        self.replication_factor = 0
        
    def add_producer(self, task):
        if isinstance(task, Task) == True or isinstance(task, DataTask) == True:
            self.producers.append(task)
        else:
            print('TypeError in adding producer: Expected Task, found {}'.format(task))
            sys.exit(-1)

    def add_consumer(self, task):
        if isinstance(task, Task) == True or isinstance(task, DataTask) == True:
            self.consumers.append(task)
        else:
            print('TypeError in adding producer: Expected Task, found {}'.format(task))
            sys.exit(-1)

    '''
    persist the VDO outside of the VDS
    '''
    def persist(self, persistence):
        self.persistence = bool(persistence)
        #self.persistent_storage = storage_ids


    '''
    set replication factor for the VDO
    '''
    def replicate(self, replication_factor):
        self.replication_factor = int(replication_factor)
    

    def set_size(self, size):
        self.size = float(size)

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
        if isinstance(vdo, VirtualDataObject):
            self.__vdo_dict__[vdo.__id__] = vdo
            self.vdos.append(vdo.__id__)
        else:
            print("Incorrect type to add to VDS! Expected `VirtualDataObject', found {}".format(type(vdo)))

    '''
    copies a VDO to another VDO in the VDS
    '''
    def copy(self, vdo_src, dest_id):
        relative_path = vdo_src.relative_path                
        vds_path = os.path.join(__VDS_MOUNTPT__, relative_path)
        vdo_id = md5hash(dest_id, vds_path)
        exists_vdo = self.vdo_exists(vdo_id)
        if exists_vdo:
            return self.__vdo_dict__[vdo_id]

        storage_hierarchy = storage_plugin.get_hierarchy()
        dest_mountpt = storage_plugin.get_storage_path(storage_hierarchy, dest_id)
        dest_path = os.path.join(dest_mountpt, relative_path)
        vdo = VirtualDataObject(dest_path)
        vdo_src.copyTo.append(vdo.__id__)
        vdo.copyFrom = vdo_src.__id__
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
    retrieve one
    '''
    def get_vdo(self, vdo_id):
        if vdo_id in self.__vdo_dict__:
            return self.__vdo_dict__[vdo_id]
        else:
            print("VDO ({}) does not exist!".format(vdo_id))
            return None

    '''
    retrieve all
    '''
    def get_vdo_list(self):
        vdo_list = [self.__vdo_dict__[id] for id in self.__vdo_dict__]
        return vdo_list

    '''
    search
    '''
    def vdo_exists(self, vdo_id):
        if vdo_id in self.__vdo_dict__:
            return True
        else:
            return False
    '''
    def vdo_exists(self, storage_id, relative_path):
        vds_path = os.path.join(__VDS_MOUNTPT__, relative_path)
        vdo_id = md5hash(storage_id, vds_path)
        if vdo_id in self.__vdo_dict__:
            return vdo_id
        else:
            return None
    '''
