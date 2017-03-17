"""
a compute-task and data-task specification to be used by VDS Coordinator
"""

from madats.core.vds import VirtualDataObject
import uuid
import storage.storage_manager as storage_manager

class Task():
    COMPUTE = 0
    DATA = 1

    def __init__(self, type = COMPUTE, **kwargs):
        self.__id__  = uuid.uuid4()
        if 'command' in kwargs:
            self.command = kwargs['command']
        else:
            self.command = ''
        self.inputs = []
        self.outputs = []
        self.params = []
        self.partition = ''
        self.walltime = ''
        self.cpus = 0
        self.account = ''
        self.predecessors = []
        self.successors = []
        self.bin = 0
        self.type = type
        self.kwargs = kwargs

    '''
    def get_remapped_params(self):
        params = []
        for param in self.params:
            if isinstance(param, VirtualDataObject):
                vdo_path = storage_manager.get_storage_path(param.storage_id)
                vdo_data = param.relative_path
                vdo = vdo_path + vdo_data
                params.append(vdo)
            else:
                params.append(param)
        return params
    '''
##########################################################################

class DataTask(Task):
    def __init__(self, src, dest, **kwargs):
        print('Datatask: {} {}'.format(src, dest))
        Task.__init__(self, Task.DATA, **kwargs)
        self.params = [src, dest]
        '''
        self.src_vdo = src
        self.dest_vdo = dest
        #self.args = args # deadline, bandwidth, latency, persist, lifetime
        self.__set_args__()
        '''

    '''
    def __set_args__(self):
        src_path = storage_manager.get_storage_path(self.src_vdo.storage_id)
        src_data = self.src_vdo.relative_path
        src = src_path + src_data

        dest_path = storage_manager.get_storage_path(self.dest_vdo.storage_id)
        dest_data = self.dest_vdo.relative_path
        dest = dest_path + dest_data
        self.params = [src, dest] # if there are additional args, set it up here
    '''
