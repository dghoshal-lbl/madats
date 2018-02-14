from madats import VirtualDataSpace, VirtualDataObject, Task
from madats import Persistence
from madats import Policy
#from madats import VirtualDataSpace, VirtualDataObject
#from madats import Task, DataTask
import madats

def main():
    vds = VirtualDataSpace()
    #vds.data_management_policy = Policy.WORKFLOW_AWARE
    print('Data management policy: {}'.format(Policy.name(vds.data_management_policy)))
    vdo = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/testfile')
    vdo.persistence = Persistence.LONG_TERM
    print('Retention time: {} s'.format(vdo.persistence))
    
    task = Task(command='echo')
    task.inputs = ['hello', 'world', vdo]
    task1 = Task(command='echo')
    task1.inputs = ['foo', 'bar', vdo]
    vds.add(task)
    vds.add(task1)
    #task.inputs = [vdo]

    #madats.plan(vds)
    madats.manage(vds)
    #madats.destroy(vds)

if __name__ == '__main__':
    main()
