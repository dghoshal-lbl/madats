from madats import Scheduler
from madats import Persistence
from madats import Policy
from madats import VirtualDataSpace, VirtualDataObject
from madats import Task, DataTask
import madats

def main():
    vds = madats.initVDS()
    vds.data_management_policy = Policy.STORAGE_AWARE
    print('Data management policy: {}'.format(Policy.name(vds.data_management_policy)))
    vdo = vds.create_vdo('/Users/DGhoshal/workdir/uda/css/testfile')
    vdo.persistence = Persistence.LONG_TERM
    print('Retention time: {} s'.format(vdo.persistence))
    
    data2 = '/Users/DGhoshal/workdir/uda/css/test2'
    vdo2 = vds.create_vdo(data2)
    task = Task(command='echo')
    #task.params = ['hello', 'world']
    task.inputs = ['hello', 'world', vdo, data2]
    #vdo.add_consumer(task)

    madats.plan(vds)
    madats.manage(vds)
    madats.destroy(vds)

if __name__ == '__main__':
    main()
