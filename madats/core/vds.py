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
try:
    from os import scandir
except ImportError:
    from scandir import scandir

class VirtualDataObject(object):
    """
    A virtual data object represents the data in VDS; encapsulates producer and consumer tasks
    """
    
    def __init__(self, datapath):        
        # a virtual data object abstraction
        self._abspath = os.path.abspath(datapath)
        self.__id__ = storage.get_data_id(self._abspath) # get the MD5 hash of the datapath string
        self._storage_id, self._relative_path = storage.get_path_elements(self._abspath)

        self._producers = []
        self._consumers = []

        # data properties that impact data management decisions
        self._size = self.__set_default_size__()  # size in bytes
        self._persistence = Persistence.NONE
        self._persist = False
        self._replication = 0
        self._deadline = 0 # epoch_time_in_ms
        self._destination = ''
        self._qos = {} 

        self.copy_to = []
        self.copy_from = None
        self.is_temporary = False
        
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
        self._producers = []
        if type(tasks) == list:
            for task in tasks:
                if isinstance(task, Task):
                    if task not in self._producers:
                        self._producers.append(task)
                else:
                    print("Invalid task type")
                    sys.exit()
        else:
            if isinstance(tasks, Task):
                if tasks not in self._producers:
                    self._producers = [tasks]
            else:
                print("Invalid task type")
                sys.exit()

    @property
    def consumers(self):
        return self._consumers

    @consumers.setter
    def consumers(self, tasks):
        self._consumers = []
        if type(tasks) == list:
            for task in tasks:
                if isinstance(task, Task):
                    if task not in self._consumers:
                        self._consumers.append(task)
                else:
                    print("Invalid task type")
                    sys.exit()
        else:
            if isinstance(tasks, Task):
                if tasks not in self._consumers:
                    self._consumers = [tasks]
            else:
                print("Invalid task type")
                sys.exit()


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
        if isinstance(task, Task):
            if task not in self._consumers:
                self._consumers.append(task)
        else:
            print("Invalid task type")
            sys.exit()

    def add_producer(self, task):
        if isinstance(task, Task):
            if task not in self._producers:
                self._producers.append(task)
        else:
            print("Invalid task type")
            sys.exit()

    def __set_default_size__(self):
        size = 0
        if os.path.exists(self.abspath):
            size = self.__get_size__(self.abspath)
        return size

    def __get_size__(self, path):
        total_size = 0
        if not os.path.exists(path):
            return 0
        elif os.path.isdir(path):
            for entry in scandir(path):
                if entry.is_dir(follow_symlinks=False):
                    total_size += self.__get_size__(entry.path)
                else:
                    total_size += entry.stat(follow_symlinks=False).st_size
        else:
            total_size = os.path.getsize(path)
        return total_size

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
        self.datapaths = {}
        self._data_management_policy = Policy.NONE
        self._storage_tiers = {}
        self.__datatasks__ = {}
        self._auto_cleanup = False

    @property
    def vdos(self):
        return self._vdos

    ### Basic Operations ###
    '''
    maps a datapath to a VDO: creates and adds a VDO in the VDS
    '''
    def map(self, datapath):
        abspath = os.path.abspath(datapath)
        if abspath in self.datapaths:
            return self.datapaths[abspath]

        vdo = VirtualDataObject(abspath)
        self.vdos.append(vdo)
        self.datapaths[abspath] = vdo
        vdo_id = storage.get_data_id(abspath) 
        self.__vdo_dict__[vdo_id] = vdo
        return vdo

    '''
    adds a VDS object (task/VDO) to the VDS
    '''
    def add(self, vds_obj):
        obj_type = type(vds_obj)
        if obj_type == VirtualDataObject:
            self.__add_vdo__(vds_obj)
        else:
            print('Invalid object type {}. VDS can only contain virtual data objects'.format(obj_type))
            return


    '''
    adds a VDO to the VDS 
    '''
    def __add_vdo__(self, vdo):
        # only add vdo when it's not already in vds
        if self.vdo_exists(vdo.__id__):
            print("Virtual data object for {} already exists".format(vdo.abspath))
        else:
            self.vdos.append(vdo)
            self.datapaths[vdo.abspath] = vdo
            self.__vdo_dict__[vdo.__id__] = vdo


    '''
    copies a VDO to another VDO in the VDS: copies all associated attributes and associations
    '''
    def copy(self, vdo_src, dest_id):
        relative_path = vdo_src.relative_path
        dest_path = storage.build_data_path(dest_id, relative_path)
        vdo_id = storage.get_data_id(dest_path)
        if self.vdo_exists(vdo_id):
             return self.__vdo_dict__[vdo_id]

        vdo = self.map(dest_path)
        vdo_src.copy_to.append(vdo)
        vdo.copy_from = vdo_src
        vdo.consumers = [cons for cons in vdo_src.consumers]
        vdo.producers = [prod for prod in vdo_src.producers]
        #self.__vdo_dict__[vdo_id] = vdo
        return vdo


    '''
    replaces a VDO with another VDO
    '''
    def replace(self, vdo_src, vdo_dest):
        for task in vdo_dest.consumers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == vdo_src:
                    task.params[i] = vdo_dest            
        for task in vdo_dest.producers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == vdo_src:
                    task.params[i] = vdo_dest            
        print('Changing datapath from {} to {}'.format(vdo_src.abspath, vdo_dest.abspath))
        self.delete(vdo_src)


    '''
    deletes a VDO from the VDS
    '''
    def delete(self, vdo):
        del self.__vdo_dict__[vdo.__id__]
        self._vdos.remove(vdo)


    '''
    check for a VDO
    '''
    def vdo_exists(self, vdo_id):
        if vdo_id in self.__vdo_dict__:
            return True
        else:
            return False

    ### Data Management Operations ###
    """
    creates a data task to move a virtual data object to a different storage layer(s)
    """
    def create_data_task(self, vdo_src, vdo_dest, **kwargs):
        if not self.vdo_exists(vdo_src.__id__):
            self.__add_vdo__(vdo_src)
        if not self.vdo_exists(vdo_dest.__id__):
            self.__add_vdo__(vdo_dest)
        src_data = vdo_src.abspath
        dest_data = vdo_dest.abspath
        # if staging in data, vdo_src has no producers
        if len(vdo_src.producers) == 0 and len(vdo_src.consumers) > 0:
            """
            if the source is not on archive and is already staged-in with
            the `latest` data then do not stage-in again.
            if the source is in on archive or the data is stale, then
            stage-in
            """
            if vdo_src.storage_id != 'archive' and storage.is_same(vdo_src.abspath, vdo_dest.abspath):
                print("No data movement necessary, {} == {}".format(vdo_src.abspath, vdo_dest.abspath))
                self.replace(vdo_src, vdo_dest)
                vdo_dest.is_temporary = True
                return

            """
            check the datatask-id and create a datatask
            """
            dt_id = self.__get_datatask_id__(vdo_src, vdo_dest)            
            if self.__datatask_exists__(dt_id):
                print('Data task ({}) already exists'.format(dt_id))
                return
            else:
                print('Creating data stage-in task ({} -> {})'.format(src_data, dest_data))

            """
            update the I/O parameters if data is moved
            """
            for task in vdo_dest.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest
            for task in vdo_dest.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest

            data_task = DataTask(dt_id, vdo_src, vdo_dest, **kwargs)
            self.__datatasks__[dt_id] = data_task
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
        # if staging out data, vdo_src has no consumers
        elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0) or \
                (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
            """
            stage-out if the output data changed
            """
            if vdo_src.storage_id != 'archive' and storage.is_same(vdo_src.abspath, vdo_dest.abspath):
                print("No data movement necessary, {} == {}".format(vdo_src.abspath, vdo_dest.abspath))
                self.replace(vdo_src, vdo_dest)
                vdo_dest.is_temporary = True
                return

            dt_id = self.__get_datatask_id__(vdo_src, vdo_dest)            
            if self.__datatask_exists__(dt_id):
                print('Data task ({}) already exists'.format(dt))
                return
            else:
                print('Data stage-out task ({} -> {}) created'.format(src_data, dest_data))
                
            """
            update the I/O paramters to use the moved data
            """
            for task in vdo_src.consumers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest
            for task in vdo_src.producers:
                params = task.params
                for i in range(len(params)):
                    if task.params[i] == vdo_src:
                        task.params[i] = vdo_dest

            """
            create a data task and add it to the respective VDOs
            """
            data_task = DataTask(dt_id, vdo_dest, vdo_src, **kwargs)
            self.__datatasks__[dt_id] = data_task
            
            vdo_dest.producers = [data_task]        
            vdo_src.consumers.append(data_task)
            vdo_dest.consumers = []
        # for non-persistent intermediate data
        else:
            self.replace(vdo_src, vdo_dest)
        '''
        setting vdo type to temporary because this VDO is created as part of a data-task
        and hence, the actual physical data may not be persisted (depends on the VDO properties)
        '''
        vdo_dest.is_temporary = True


    '''
    cleanup task that automatically removes unused data mapped to a VDO
    '''
    def create_cleanup_task(self, vdo):
        if not vdo.persist and vdo.is_temporary:
            dummy_vdo_path = vdo.abspath + '.deleted'
            dummy_vdo = self.map(dummy_vdo_path)
            for consumer in vdo.consumers:
                dummy_vdo.add_producer(consumer)
            
            cleanup_task = CleanupTask(vdo)
            dummy_vdo.add_consumer(cleanup_task)


    '''
    setup auto cleanup
    '''
    @property
    def auto_cleanup(self):
        return self._auto_cleanup

    @auto_cleanup.setter
    def auto_cleanup(self, auto_cleanup):
        self._auto_cleanup = auto_cleanup

    '''
    check for a data task
    '''
    def __datatask_exists__(self, datatask_id):
        if datatask_id in self.__datatasks__:
            return True
        else:
            return False

    '''
    calculate data-task id based on the src and dest data
    '''
    def __get_datatask_id__(self, vdo_src, vdo_dest):
            return (vdo_src.__id__ + vdo_dest.__id__)


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


    """
    returns a task view of the VDS
    """
    def get_task_dag(self):
        dag = {}
        for vdo in self.vdos:
            for prod in vdo.producers:
                if prod not in dag:
                    dag[prod] = []
                for cons in vdo.consumers:
                    '''
                    - add the dependencies for each task
                    - avoid self-dependencies to avoid deadlock  
                    '''
                    if cons not in dag[prod] and cons != prod:
                        dag[prod].append(cons)
                        cons.add_predecessor(prod)
                        prod.add_successor(cons)

            for con in vdo.consumers:
                if con not in dag:
                    dag[con] = []
        return dag

##############################################################################

class Task(object):
    """
    A workflow task object that corresponds to a single stage/step/task/job in the workflow
    - A task can only have a command, parameters (params) and attributes
    - Since VDS' model is data-centric, there is no input/output to a task, but only params
    - However, VDOs have producers and consumers that help VDS to build the dependencies
    """

    def __init__(self, command, type=TaskType.COMPUTE):
        self.__id__  = str(uuid.uuid4())
        self._name = self.__id__
        self._command = command
        self._params = []
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
    def params(self):
        return self._params

    @params.setter
    def params(self, params):
        self._params = params

#    @property
#    def outputs(self):
#        return self._outputs

#    @outputs.setter
#    def outputs(self, outputs):
#        self._outputs = outputs
#        for output in outputs:
#            if isinstance(output, VirtualDataObject):
#                output.add_producer(self)

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
        if t not in self.predecessors:
            self.predecessors.append(t)

    def add_successor(self, t):
        if t not in self.successors:
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
    A datatask that is specifically marked to move data objects between storage tiers.
    This is the 
    """

    def __init__(self, id, vdo_src, vdo_dest):
        Task.__init__(self, command='', type=TaskType.DATA)        
        self.__id__ = id
        self.params = [vdo_src, vdo_dest]
        self.command = self.__set_data_mover__(vdo_src, vdo_dest)


    def get_datatask_id(self):
        return self.__id__


    """
    data mover that copies data based on the storage tier
    """
    def __set_data_mover__(self, vdo_src, vdo_dest):
        # the default data mover is 'cp', however, it
        # changes based on the source and destination
        # storage/file systems
        dest_directory = os.path.dirname(vdo_dest.abspath)
        if vdo_src.storage_id == 'archive':
            # hack: using ls -lrt at the end so that using
            # vdo_src and vdo_dest params work in general
            # through the execution manager, which appends
            # the task inputs at the end of the command
            command = 'mkdir -p {}; cd {}; hsi -q "prompt; mget {}"; ls '.format(dest_dir, vdo_dest.abspath, vdo_src.abspath)
        elif vdo_dest.storage_id == 'archive':
            src_dir = os.path.dirname(vdo_src.abspath)
            filename = os.path.basename(vdo_dest.abspath)
            command = 'cd {}; hsi -q "prompt; mkdir -p {}; cd {}; mput {}"; ls '.format(src_dir, dest_dir, dest_dir, filename)
        else:
            command = 'mkdir -p {}; cp -R '.format(dest_directory)

        return command
        
##########################################################################
class CleanupTask(Task):
    """
    A datatask that is specifically marked to move data objects between storage tiers.
    This is the 
    """

    def __init__(self, vdo):
        Task.__init__(self, command='rm -rRf ', type=TaskType.CLEANUP)        
        self.params = [vdo]

