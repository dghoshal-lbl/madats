"""
`madats.core.vds`
====================================

.. currentmodule:: madats.core.vds

:platform: Unix, Mac
:synopsis: Module that defines a Virtual Data Space (VDS)

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import os
import sys
import hashlib
import uuid
from madats.utils.constants import TaskType, Persistence, Policy, UNKNOWN
from madats.core.scheduler import Scheduler
from madats.core import storage

class VirtualDataObject(object):
    """
    A virtual data object represents the data in VDS; encapsulates producer and consumer tasks
    """
    
    def __init__(self, datapath):        
        self.__id__ = storage.get_data_id(datapath) # get the MD5 hash of the datapath string
        storage_id, relative_path = storage.get_elements(datapath)
        self._storage_id = storage_id
        self._relative_path = relative_path

        # a virtual data object abstraction
        self._abspath = os.path.abspath(datapath)
        self._producers = []
        self._consumers = []

        # data properties that impact data management decisions
        self._size = 0.0  # size in MB
        self._persistence = Persistence.NONE
        self._replication = 0
        self._deadline = 0 # epoch_time_in_ms
        self._destination = ''
        self._qos = {} 

        self.copy_to = []
        self.copy_from = None
        
    @property
    def storage_id(self):
        return self._storage_id

    @property
    def relative_path(self):
        return self._relative_path

    @property
    def abspath(self):
        return self._abspath
    
    @property
    def producers(self):
        return self._producers

    @producers.setter
    def producers(self, tasks):
        if type(t) == list:
            self._producers = tasks
        else:
            self._producers = [tasks]

    @property
    def consumers(self):
        return self._consumers

    @consumers.setter
    def consumers(self, tasks):
        if type(t) == list:
            self._consumers = tasks
        else:
            self._consumers = [tasks]

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, size):
        self._size = size

    @property
    def persistence(self):
        return self._persistence

    @persistence.setter
    def persistence(self, persistence):
        self._persistence = persistence

    @property
    def replication(self):
        return self._replication

    @replication.setter
    def replication(self, replication):
        self._replication = replication

    @property
    def deadline(self):
        return self._deadline

    @deadline.setter
    def deadline(self, deadline):
        self._deadline = deadline

    @property
    def destination(self):
        return self._destination

    @destination.setter
    def destination(self, destination):
        self._destination = destination

    @property
    def qos(self):
        return self._qos

    @qos.setter
    def qos(self, **qos):
        self._qos = qos

######################################################################################
class VirtualDataSpace():
    """
    A virtual data space (VDS) abstraction that is a collection of virtual data objects (VDO)

    A VDS consists of VDOs and provides the following operations:
      VDO management operations:
      - add: adds a vdo to VDS
      - copy: copies a vdo into another vdo, copies the producers and consumers of the src vdo as well
      - delete: deletes a vdo
      VDO query operations: 
      - retrieve: returns the list of vdos in vds
      - search: returns a vdo, if exists, for a data object
    """
    def __init__(self):
        self.__vdo_dict__ = {}
        self.vdos = []
        self.tasks = []
        self._data_management_policy = Policy.NONE
    
    '''
    creates a Task in the VDS
    '''
    def create_task(self, command, type=Task.COMPUTE):
        task = Task(command, type)
        self.tasks.append(task)
        return task

    '''
    creates and adds a VDO in the VDS
    '''
    def create_vdo(self, datapath):
        vdo = VirtualDataObject(datapath)
        self.vdos.append(vdo)
        return vdo

    '''
    copies a VDO to another VDO in the VDS
    '''
    def copy(self, vdo_src, dest_id):
        relative_path = vdo_src.relative_path                
        dest_path = storage.build_data_path(dest_id, relative_path)
        vdo_id = storage.get_data_id(dest_path)
        if self.vdo_exists(vdo_id):
            return self.__vdo_dict__[vdo_id]

        vdo = self.create_vdo(vdo)
        vdo_src.copy_to.append(vdo)
        vdo.copy_from = vdo_src
        vdo.consumers = [cons for cons in vdo_src.consumers]
        vdo.producers = [prod for prod in vdo_src.producers]
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
    data management policy is a VDS property that defines data movement policies in the VDS
    '''
    @property
    def data_management_policy(self):
        return self._data_management_policy

    @data_management_policy.setter
    def data_management_policy(self, policy):
        ## TODO: type-check
        self._data_management_policy = policy

