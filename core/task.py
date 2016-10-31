"""
a compute-task and data-task specification to be used by VDS Coordinator
"""

class Task():
    def __init__(self):
        self.command = None
        self.partition = None
        self.walltime = None
        self.cpus = 0
        self.account = None
        self.predecessors = []
        self.successors = []

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
        self.predecessors = pred

    def set_successors(self, succ):
        self.successors = succ

##########################################################################

class DataTask(Task):
    NA = -1
    IN = 0
    OUT = 1
    INOUT = 2

    def __init__(self, src, dest, **kwargs):
        Task.__init__()
        self.src_vdo = src
        self.dest_vdo = dest
        self.deadline = None
        self.io = NA
        self.kwargs = kwargs
