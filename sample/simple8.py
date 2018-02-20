from madats import VirtualDataSpace, VirtualDataObject, Task
from madats import Persistence
from madats import Policy
#from madats import VirtualDataSpace, VirtualDataObject
#from madats import Task, DataTask
import madats

def main():
    # create a VDS
    vds = VirtualDataSpace()
    vds.data_management_policy = Policy.STORAGE_AWARE
    #vds.auto_cleanup = True
    print('Data management policy: {}'.format(Policy.name(vds.data_management_policy)))

    # create VDOs
    vdo = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/testfile')
    #vdo.non_movable = True
    #vdo.persistence = Persistence.LONG_TERM
    vdo1 = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/intfile')

    # create tasks
    task = Task(command='echo')
    task.params = ['hello', 'world', vdo, vdo1]
    task1 = Task(command='echo')
    task1.params = ['foo', 'bar', vdo1]
    
    # define VDO and task associations
    vdo.consumers = [task]
    vdo1.producers = [task]
    vdo1.consumers = [task1]
    vds.add(vdo)
    vds.add(vdo1)

    # manage VDS
    madats.manage(vds)

if __name__ == '__main__':
    main()
