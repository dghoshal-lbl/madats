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
        self._size = self._set_default_size()  # size in bytes
        self._persistence = Persistence.NONE
        self._persist = False
        self._replication = 0
        self._deadline = 0 # epoch_time_in_ms
        self._destination = ''
        self._qos = {} 
        self._non_movable = False # if the vdo is non-movable, then the data management strategy will not affect its location

        self.copy_to = []
        self.copy_from = None
        self.__is_temporary__ = False # if the vdo is temporary, then auto-cleanup will remove the data
        
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

    #@persist.setter
    #def persist(self, persist):
    #    self._persist = persist

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

    @property
    def non_movable(self):
        return self._non_movable

    @non_movable.setter
    def non_movable(self, non_movable):
        self._non_movable = non_movable

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

    def _set_default_size(self):
        size = 0
        if os.path.exists(self.abspath):
            size = self._get_size(self.abspath)
        return size

    def _get_size(self, path):
        total_size = 0
        if not os.path.exists(path):
            return 0
        elif os.path.isdir(path):
            for entry in scandir(path):
                if entry.is_dir(follow_symlinks=False):
                    total_size += self._get_size(entry.path)
                else:
                    total_size += entry.stat(follow_symlinks=False).st_size
        else:
            total_size = os.path.getsize(path)
        return total_size

######################################################################################
class VirtualDataSpace(object):
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
        self._strategy = Policy.NONE
        self._storage_tiers = {}
        self.__datatasks__ = {}
        self._auto_cleanup = False

        # basic lookup keys, more can be added later
        self.__query_elements__ = {'num_vdos': 0, 'data_tasks': 0, 'data_movements': 0,
                                   'preparer_tasks': 0, 'cleanup_tasks': 0,
                                   'auto_cleanup': False,
                                   'policy': self._strategy}

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
        self.__query_elements__['num_vdos'] += 1
        return vdo

    '''
    adds a VDS object (task/VDO) to the VDS
    '''
    def add(self, vds_obj):
        obj_type = type(vds_obj)
        if obj_type == VirtualDataObject:
            self._add_vdo(vds_obj)
        else:
            print('Invalid object type {}. VDS can only contain virtual data objects'.format(obj_type))
            return


    '''
    adds a VDO to the VDS 
    '''
    def _add_vdo(self, vdo):
        # only add vdo when it's not already in vds
        if self.vdo_exists(vdo.__id__):
            print("Virtual data object for {} already exists".format(vdo.abspath))
        else:
            self.vdos.append(vdo)
            self.datapaths[vdo.abspath] = vdo
            self.__vdo_dict__[vdo.__id__] = vdo
            self.__query_elements__['num_vdos'] += 1


    '''
    copies a VDO to another VDO in the VDS: copies all associated attributes and associations
    '''
    def copy(self, vdo_src, dest_id):
        relative_path = vdo_src.relative_path
        dest_path = storage.build_data_path(dest_id, relative_path)
        vdo_id = storage.get_data_id(os.path.abspath(dest_path))
        if self.vdo_exists(vdo_id):
             return self.__vdo_dict__[vdo_id]

        vdo = self.map(dest_path)
        vdo_src.copy_to.append(vdo)
        vdo.copy_from = vdo_src
        vdo.consumers = [cons for cons in vdo_src.consumers]
        vdo.producers = [prod for prod in vdo_src.producers]
        #self.__vdo_dict__[vdo_id] = vdo
        self._create_data_task(vdo_src, vdo)
        return vdo


    '''
    replaces a VDO with another VDO
    '''
    def replace(self, old_vdo, new_vdo):
        for task in new_vdo.consumers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == old_vdo:
                    task.params[i] = new_vdo            
        for task in new_vdo.producers:
            params = task.params
            for i in range(len(params)):
                if task.params[i] == old_vdo:
                    task.params[i] = new_vdo            
        print('Changing datapath from {} to {}'.format(old_vdo.abspath, new_vdo.abspath))
        self.delete(old_vdo)


    '''
    deletes a VDO from the VDS
    '''
    def delete(self, vdo):
        if vdo.__id__ in self.__vdo_dict__:
            del self.__vdo_dict__[vdo.__id__]
            del self.datapaths[vdo.abspath]
            self._vdos.remove(vdo)
            self.__query_elements__['num_vdos'] -= 1


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
    def _create_data_task(self, vdo_src, vdo_dest, **kwargs):
        # if the vdo is not movable, then no data task is needed, nor the dest vdo
        if vdo_src.non_movable:
            self.delete(vdo_dest)
            return

        if not self.vdo_exists(vdo_src.__id__):
            self._add_vdo(vdo_src)
        if not self.vdo_exists(vdo_dest.__id__):
            self._add_vdo(vdo_dest)
        src_data = vdo_src.abspath
        dest_data = vdo_dest.abspath
        # if staging in data, vdo_src has no producers: vdo_src -> vdo_dest
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
                vdo_dest.__is_temporary__ = True
                return

            """
            check the datatask-id and create a datatask
            """
            dt_id = self._get_datatask_id(vdo_src, vdo_dest)            
            if self._datatask_exists(dt_id):
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
            self.__query_elements__['data_movements'] += 1
            """
            - data stagein task becomes the consumer of the original data
            - data stagein task becomes the producer of the new data
            - consumers of the original data become the consumers of the new data
            """
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
            """
            create a data preparer task to make the directory for vdo_dest
            """
            dest_dir_path = os.path.dirname(vdo_dest.abspath)
            vdo_dest_dir = self.map(dest_dir_path)
            data_preparer_id = self._get_datatask_id(None, vdo_dest_dir, 'preparer')
            '''
            - if the data preparer task is new, then create it and add as the
              producer of vdo_dest
            - if the data preparer task already exists, then add it as the
              producer of vdo_dest 
            '''
            if not self._datatask_exists(data_preparer_id):
                preparer_task = DataTask(data_preparer_id, None, vdo_dest_dir, DataTask.PREPARER)
                self._assign_preparer_task_dependency(vdo_dest_dir, vdo_dest, preparer_task)
                self.__datatasks__[data_preparer_id] = preparer_task
                print('Data preparer task created: {}'.format(dest_dir_path))
                self.__query_elements__['preparer_tasks'] += 1
            else:
                vdo_dest_dir.add_consumer(data_task)
        # if staging out data, vdo_src has no consumers: vdo_src <- vdo_dest
        elif (len(vdo_src.consumers) == 0 and len(vdo_src.producers) > 0) or \
                (len(vdo_src.consumers) > 0 and len(vdo_src.producers) > 0 and vdo_src.persist == True):
            """
            always stage-out the data because the data may be changed when the computation ends
            """
            #if vdo_src.storage_id != 'archive' and storage.is_same(vdo_src.abspath, vdo_dest.abspath):
            #    print("No data movement necessary, {} == {}".format(vdo_src.abspath, vdo_dest.abspath))
            #    self.replace(vdo_src, vdo_dest)
            #    vdo_dest.__is_temporary__ = True
            #    return

            dt_id = self._get_datatask_id(vdo_src, vdo_dest)            
            if self._datatask_exists(dt_id):
                print('Data task ({}) already exists'.format(dt_id))
                return
            else:
                print('Data stage-out task ({} -> {}) created'.format(dest_data, src_data))
                
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
            self.__query_elements__['data_movements'] += 1
            '''
            since vdo_src is where the final output should be while staging out,
            it's producer is the data task; while all the compute tasks actually
            write the data through vdo_dest which is then staged out to vdo_src 
            '''
            vdo_src.producers = [data_task]        
            vdo_dest.consumers.append(data_task)
            vdo_src.consumers = []
            """
            create a data preparer task to make the directory for vdo_dest
            """
            src_dir_path = os.path.dirname(vdo_src.abspath)
            vdo_src_dir = self.map(src_dir_path)
            data_preparer_id = self._get_datatask_id(None, vdo_src_dir, 'preparer')
            if not self._datatask_exists(data_preparer_id):
                preparer_task = DataTask(data_preparer_id, None, vdo_src_dir, DataTask.PREPARER)
                self._assign_preparer_task_dependency(vdo_src_dir, vdo_src, preparer_task)
                self.__datatasks__[data_preparer_id] = preparer_task
                print('Data preparer task created: {}'.format(src_dir_path))
                self.__query_elements__['preparer_tasks'] += 1
            else:
                vdo_src_dir.add_consumer(data_task)
        # for non-persistent intermediate data: vdo_src <-> vdo_dest
        else:
            if vdo_src.storage_id != 'archive' and storage.is_same(vdo_src.abspath, vdo_dest.abspath):
                print("No data preparation necessary, {} == {}".format(vdo_src.abspath, vdo_dest.abspath))
                self.replace(vdo_src, vdo_dest)
                vdo_dest.__is_temporary__ = True
                return

            '''
            Simpler: create a vdo with destination's parent directory that will be
            created before the destination data gets created
            - appends the creation of the parent directory before the replaced vdo
            - ensures executing the preparer datatask prior to using the replaced vdo
            '''
            dest_dir_path = os.path.dirname(vdo_dest.abspath)
            vdo_dest_dir = self.map(dest_dir_path)
            data_preparer_id = self._get_datatask_id(None, vdo_dest_dir, 'preparer')
            if not self._datatask_exists(data_preparer_id):
                preparer_task = DataTask(data_preparer_id, None, vdo_dest_dir, DataTask.PREPARER)
                self._assign_preparer_task_dependency(vdo_dest_dir, vdo_dest, preparer_task)
                self.__datatasks__[data_preparer_id] = preparer_task
                print('Data preparer task created: {}'.format(dest_dir_path))
                self.__query_elements__['preparer_tasks'] += 1
            else:
                for producer in vdo_dest.producers:
                    vdo_dest_dir.add_consumer(producer)
            self.replace(vdo_src, vdo_dest)
                
        '''
        setting vdo type to temporary because this VDO is created as part of a data-task
        and hence, the actual physical data may not be persisted (depends on the VDO properties)
        '''
        vdo_dest.__is_temporary__ = True
        if self.auto_cleanup:
            self._create_cleanup_task(vdo_dest)


    '''
    create a dummy vdo and assign data preparer task as producer and consumer 
    '''
    def _assign_preparer_task_dependency(self, vdo_src, vdo_dest, preparer_task):
        vdo_src.add_producer(preparer_task)
        for producer in vdo_dest.producers:
            vdo_src.add_consumer(producer)


    '''
    cleanup task that automatically removes unused data mapped to a VDO
    '''
    def _create_cleanup_task(self, vdo):
        if not vdo.persist and vdo.__is_temporary__:
            print("******* Path {} will be removed ******".format(vdo.abspath))
            dummy_vdo_path = vdo.abspath + '.deleted'
            dummy_vdo = self.map(dummy_vdo_path)
            dt_id = self._get_datatask_id(vdo, dummy_vdo)
            if not self._datatask_exists(dt_id):
                for consumer in vdo.consumers:
                    dummy_vdo.add_producer(consumer)
                for producer in vdo.producers:
                    dummy_vdo.add_producer(producer)            
                cleanup_task = DataTask(dt_id, vdo, dummy_vdo, DataTask.CLEANER)
                self.__datatasks__[dt_id] = cleanup_task
                #cleanup_task = CleanupTask(vdo)
                dummy_vdo.add_consumer(cleanup_task)
                self.__query_elements__['cleanup_tasks'] += 1



    '''
    setup auto cleanup
    '''
    @property
    def auto_cleanup(self):
        return self._auto_cleanup

    @auto_cleanup.setter
    def auto_cleanup(self, auto_cleanup):
        self.__query_elements__['auto_cleanup'] = auto_cleanup
        #print("AUTO_CELANUP: {}".format(auto_cleanup))
        self._auto_cleanup = auto_cleanup

    '''
    check for a data task
    '''
    def _datatask_exists(self, datatask_id):
        if datatask_id in self.__datatasks__:
            return True
        else:
            self.__query_elements__['data_tasks'] += 1
            return False

    '''
    calculate data-task id based on the src and dest data
    '''
    def _get_datatask_id(self, vdo_src, vdo_dest, task_type=''):
        if vdo_src is not None and vdo_dest is not None:
            return (vdo_src.__id__ + vdo_dest.__id__)
        elif vdo_src is None:
            if task_type != '':
                return vdo_dest.__id__ + task_type
            else:
                print('Task type is required if vdo_src is None')
                sys.exit(1)
        else:
            if task_type != '':
                return vdo_src.__id__ + task_type
            else:
                print('Task type is required if vdo_dest is None')
                sys.exit(1)
            


    '''
    strategy is a VDS property that defines data movement policies in the VDS
    '''
    @property
    def strategy(self):
        return self._strategy

    @strategy.setter
    def strategy(self, strategy):
        if strategy in Policy.policies():
            self.__query_elements__['policy'] = strategy
            self._strategy = strategy
        else:
            print('Incorrect data management strategy. Setting to default: {}'.format(Policy.name(self.__query_elements__['policy'])))


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
                    #print(vdo.abspath, len(vdo.producers), len(vdo.consumers))
                    dag[con] = []
                    for prod in vdo.producers:
                        if prod not in dag:
                            dag[prod] = [con]
                        elif con not in dag[prod] and con != prod:
                            dag[prod].append(con)
                            con.add_predecessor(prod)
                            prod.add_successor(con)
        return dag

    ####### Query Interfaces #######
    """
    count(): number of virtual data objects in a VDS
    """
    def count(self):
        return self.__query_elements__['num_vdos']
    

    """
    data() : a listof datapaths mapped to the VDS
    """
    def data(self):
        data_elements = []
        for vdo in self.vdos:
            data_elements.append(vdo.abspath)
        return data_elements


    """
    tasks() : dictionary of the list of tasks based on their types
    """
    def tasks(self):
        compute_tasks = {}
        data_tasks = {}
        for vdo in self.vdos:
            for prod in vdo.producers:
                if prod.type == TaskType.COMPUTE:
                    if prod.__id__ not in compute_tasks:
                        compute_tasks[prod.__id__] = self._get_task_command(prod)
                else:
                    if prod.__id__ not in data_tasks:
                        data_tasks[prod.__id__] = self._get_task_command(prod)
            for cons in vdo.consumers:
                if cons.type == TaskType.COMPUTE:
                    if cons.__id__ not in compute_tasks:
                        compute_tasks[cons.__id__] = self._get_task_command(cons)
                else:
                    if cons.__id__ not in data_tasks:
                        data_tasks[cons.__id__] = self._get_task_command(cons)

        tasks = {'compute': [], 'data': []}        
        for task in compute_tasks:
            tasks['compute'].append(compute_tasks[task])
        for task in data_tasks:
            tasks['data'].append(data_tasks[task])
        
        return tasks

    
    """
    lookup(): get specific information about the VDS
    """
    def lookup(self, key):
        if key not in self.__query_elements__:
            return None
        else:
            return self.__query_elements__[key]
    

    def _get_task_command(self, task):
        params = []
        for param in task.params:
            if type(param) == VirtualDataObject:
                params.append(param.abspath)
            else:
                params.append(param)                                
        command = "{} {}".format(task.command, ' '.join(params))
        return command

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
    PREPARER = 0 # only prepares the target data directory
    MOVER = 1    # prepares target directories and moves the data
    CLEANER = 2  # removes used up data

    def __init__(self, id, vdo_src, vdo_dest, datatask_type=MOVER):
        Task.__init__(self, command='', type=TaskType.DATA)        
        self.__id__ = id
        self._datatask_type = None
        if datatask_type == DataTask.PREPARER:
            self.params = [vdo_dest.abspath]
            self.command = "mkdir -p"
            self._datatask_type = DataTask.PREPARER
        elif datatask_type == DataTask.MOVER:
            self.params = [vdo_src, vdo_dest]
            self.command = self._set_data_mover(vdo_src, vdo_dest)
            self._datatask_type = DataTask.MOVER
        elif datatask_type == DataTask.CLEANER:
            self.params = [vdo_src]
            self.command = "rm -rRf"
            self._datatask_type = DataTask.CLEANER


    def get_datatask_id(self):
        return self.__id__

    @property
    def datatask_type(self):
        return self._datatask_type

    """
    data mover that copies data based on the storage tier
    """
    def _set_data_mover(self, vdo_src, vdo_dest):
        # the default data mover is 'cp', however, it
        # changes based on the source and destination
        # storage/file systems
        dest_directory = os.path.dirname(vdo_dest.abspath)
        if vdo_src.storage_id == 'archive':
            # hack: using ls -lrt at the end so that using
            # vdo_src and vdo_dest params work in general
            # through the execution manager, which appends
            # the task inputs at the end of the command
            command = 'mkdir -p {}; cd {}; hsi -q "prompt; mget {}"; ls'.format(dest_dir, vdo_dest.abspath, vdo_src.abspath)
        elif vdo_dest.storage_id == 'archive':
            src_dir = os.path.dirname(vdo_src.abspath)
            filename = os.path.basename(vdo_dest.abspath)
            command = 'cd {}; hsi -q "prompt; mkdir -p {}; cd {}; mput {}"; ls'.format(src_dir, dest_dir, dest_dir, filename)
        else:
            #command = 'mkdir -p {}; cp -R'.format(dest_directory)
            command = 'cp -R'.format(dest_directory)

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
