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
        self._persist = False
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
        if type(tasks) == list:
            self._producers = tasks
        else:
            self._producers = [tasks]

    @property
    def consumers(self):
        return self._consumers

    @consumers.setter
    def consumers(self, tasks):
        if type(tasks) == list:
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
    def persist(self):
        return self._persist

    @property
    def persistence(self):
        return self._persistence

    @persistence.setter
    def persistence(self, persistence):
        self._persistence = persistence
        if persistence != Persistence.NONE:
            self._persist = True

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

    def add_consumer(self, task):
        self._consumers.append(task)

    def add_producer(self, task):
        self._producers.append(task)

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
        self._vdos = []
        self._tasks = []
        self.datapaths = {}
        self._data_management_policy = Policy.NONE
    
    @property
    def vdos(self):
        return self._vdos

    @property
    def tasks(self):
        return self._tasks

    '''
    creates a Task in the VDS
    '''
    def create_task(self, command):
        task = Task(command, type)
        self._tasks.append(task)
        return task

    '''
    creates and adds a VDO in the VDS
    '''
    def create_vdo(self, datapath):
        if datapath in self.datapaths:
            return self.datapaths[datapath]

        vdo = VirtualDataObject(datapath)
        self.vdos.append(vdo)
        self.datapaths[datapath] = vdo
        vdo_id = storage.get_data_id(datapath) 
        self.__vdo_dict__[vdo_id] = vdo
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

        vdo = self.create_vdo(dest_path)
        vdo_src.copy_to.append(vdo)
        vdo.copy_from = vdo_src
        vdo.consumers = [cons for cons in vdo_src.consumers]
        vdo.producers = [prod for prod in vdo_src.producers]
        self.__vdo_dict__[vdo_id] = vdo
        return vdo

    '''
    deletes a VDO from the VDS
    '''
    def delete(self, vdo):
        del self.__vdo_dict__[vdo.__id__]
        self._vdos.remove(vdo)

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
    '''
    def get_vdo_list(self):
        vdo_list = [self.__vdo_dict__[id] for id in self.__vdo_dict__]
        return vdo_list
    '''

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


##############################################################################

class Task(object):
    """
    A workflow task object that corresponds to a single stage/step/task/job in the workflow
    """

    def __init__(self, command, type=TaskType.COMPUTE):
        self.__id__  = str(uuid.uuid4())
        self._name = self.__id__
        self._command = command
        self._inputs = []
        self._outputs = []
        self._expected_runtime = UNKNOWN
        self._priority = UNKNOWN
        self.predecessors = []
        self.successors = []
        self._bin = 0
        self._type = type
        self._scheduler = Scheduler.NONE
        self._scheduler_opts = {}
        self._prerun = []
        self._postrun = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def command(self):
        return self._command

    @command.setter
    def command(self, command):
        self._command = command

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, inputs):
        self._inputs = inputs
        for input in inputs:
            if isinstance(input, VirtualDataObject):
                input.add_consumer(self)

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, outputs):
        self._outputs = outputs
        for output in outputs:
            if isinstance(output, VirtualDataObject):
                output.add_producer(self)

    @property
    def runtime(self):
        return self._expected_runtime

    @runtime.setter
    def runtime(self, runtime):
        self._expected_runtime = runtime

    @property
    def type(self):
        return self._type

    @property
    def scheduler(self):
        return self._scheduler

    @scheduler.setter
    def scheduler(self, scheduler):
        self._scheduler = scheduler

    @property
    def bin(self):
        return self._bin

    @bin.setter
    def bin(self, bin):
        self._bin = bin

    @property
    def prerun(self):
        return self._prerun

    @prerun.setter
    def prerun(self, prerun):
        self._prerun = prerun

    @property
    def postrun(self):
        return self._postrun

    @postrun.setter
    def postrun(self, postrun):
        self._postrun = postrun

    def add_predecessor(self, t):
        self.predecessors.append(t)

    def add_successor(self, t):
        self.successors.append(t)
    
    @property
    def scheduler_opts(self):
        return self._scheduler_opts

    @scheduler_opts.setter
    def scheduler_opts(self, sched_opts):
        for k, v in sched_opts.items():
            self._scheduler_opts[k] = v

    def get_schedopt(self, opt):
        return self._scheduler_opts[opt]

##########################################################################
class DataTask(Task):
    """
    A datatask that is specifically marked to move data objects between storage tiers
    """

    def __init__(self, vdo_src, vdo_dest):
        Task.__init__(self, command='', type=TaskType.DATA)        
        self.vdo_src = vdo_src
        self.vdo_dest = vdo_dest
        self.inputs = [vdo_src, vdo_dest]
        self.outputs = [vdo_dest]
        self.command = self.__set_data_mover__(vdo_src, vdo_dest)


    def __set_data_mover__(self, vdo_src, vdo_dest):
        # the default data mover is 'cp', however, it
        # changes based on the source and destination
        # storage/file systems
        data_directory = os.path.dirname(vdo_dest.abspath)
        command = 'mkdir -p {}\n'.format(data_directory)
        command = command + 'cp -R'

        # TODO: add flexible data mover; can take it as an arg as well from the calling function

        return command
        
