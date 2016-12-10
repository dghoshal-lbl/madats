"""
a compute-task and data-task specification to be used by VDS Coordinator
"""

from vds import VirtualDataObject

class Task():
    COMPUTE = 0
    DATA = 1

    def __init__(self, id, type = COMPUTE):
        self.__id__  = id
        self.command = None
        self.partition = None
        self.walltime = None
        self.cpus = 0
        self.account = None
        self.predecessors = []
        self.successors = []
        self.bin = 0
        self.type = type

    def set_command(self, command):
        self.command = command

    def set_partition(self, partition):
        self.partition = partition

    def set_walltime(self, walltime):
        self.walltime = walltime

    def set_cpus(self, cpus):
        self.cpus = cpus

    def set_account(self, account):
        self.account = account

    def set_predecessors(self, pred):
        # todo: type-checking for task-type
        self.predecessors = pred

    def set_successors(self, succ):
        # todo: type-checking for task-type
        self.successors = succ

##########################################################################

class DataTask(Task):
    '''
    NA = -1
    IN = 0
    OUT = 1
    INOUT = 2
    '''
    def __init__(self, id, src, dest, **kwargs):
        Task.__init__(self, id, Task.DATA)
        self.src_vdo = src
        self.dest_vdo = dest
        self.deadline = None
        #self.io_direction = -1
        self.kwargs = kwargs


if __name__ == '__main__':
    vdo1 = VirtualDataObject('scratch', 'testdir/indata')
    vdo2 = VirtualDataObject('scratch', 'testdir/outdata')
    
    dt = DataTask(0, vdo1, vdo2)
    dt.set_command('cp')
    print("{} {} {} {}".format(dt.type, dt.command, dt.src_vdo.__id__, dt.dest_vdo.__id__))
