"""
a compute-task and data-task specification to be used by VDS Coordinator
"""

from madats.core.vds import VirtualDataObject
import uuid

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

##########################################################################

class DataTask(Task):
    def __init__(self, src, dest, **kwargs):
        #print('Datatask: {} {}'.format(src, dest))
        Task.__init__(self, Task.DATA, **kwargs)
        self.params = [src, dest]
