from madats import VirtualDataSpace, VirtualDataObject, Task
from madats import Persistence
from madats import Policy
import madats

def main():
    vds = VirtualDataSpace()
    vds.data_management_policy = Policy.STORAGE_AWARE
    print('Data management policy: {}'.format(Policy.name(vds.data_management_policy)))
    vdo = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/testfile')
    vdo.persistence = Persistence.LONG_TERM
    vdo1 = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/intfile')
    #print('Retention time: {} s'.format(vdo.persistence))
    
    task = Task(command='echo')
    task.params = ['hello', 'world', vdo]
    task1 = Task(command='echo')
    task1.params = ['foo', 'bar', vdo1]
    task2 = Task(command='sleep')
    task2.params = ['5']
    
    vdo.consumers = [task, task2]
    vdo1.producers = [task2]
    vdo1.consumers = [task1]
    vds.add(vdo)
    vds.add(vdo1)
    #task.inputs = [vdo]

    #madats.plan(vds)
    madats.manage(vds)
    #madats.destroy(vds)

if __name__ == '__main__':
    main()
