"""
a compute-task and data-task specification to be used by VDS Coordinator
"""

from core.vds import VirtualDataObject
import uuid
import storage.storage_manager as storage_manager

class Task():
    COMPUTE = 0
    DATA = 1

    def __init__(self, type = COMPUTE, args=[]):
        self.__id__  = uuid.uuid4()
        self.command = ''
        self.params = []
        self.partition = ''
        self.walltime = ''
        self.cpus = 0
        self.account = ''
        self.predecessors = []
        self.successors = []
        self.bin = 0
        self.type = type
        self.args = args

##########################################################################

class DataTask(Task):
    def __init__(self, src, dest, args):
        Task.__init__(self, Task.DATA, args)
        self.src_vdo = src
        self.dest_vdo = dest
        #self.args = args # deadline, bandwidth, latency, persist, lifetime
        self.__set_args__()

    def __set_args__(self):
        if 'command' in self.args:
            self.command = self.args['command']

        src_path = storage_manager.get_storage_path(self.src_vdo.storage_id)
        src_data = self.src_vdo.relative_path
        src = src_path + src_data

        dest_path = storage_manager.get_storage_path(self.dest_vdo.storage_id)
        dest_data = self.dest_vdo.relative_path
        dest = dest_path + dest_data
        self.params = [src, dest] # if there are additional args, set it up here

if __name__ == '__main__':
    vdo1 = VirtualDataObject('scratch', 'testdir/indata')
    vdo2 = VirtualDataObject('scratch', 'testdir/outdata')
    
    dt = DataTask(vdo1, vdo2, command='mv')
    print("{} {} {}".format(dt.type, dt.command, dt.params))
