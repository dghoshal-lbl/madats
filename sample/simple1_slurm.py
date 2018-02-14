from madats import VirtualDataSpace, VirtualDataObject, Task
from madats import Persistence
from madats import Policy
from madats import Scheduler
#from madats import VirtualDataSpace, VirtualDataObject
#from madats import Task, DataTask
import madats

def main():
    vds = VirtualDataSpace()
    vds.data_management_policy = Policy.STORAGE_AWARE
    print('Data management policy: {}'.format(Policy.name(vds.data_management_policy)))
    vdo = VirtualDataObject('/Users/DGhoshal/workdir/uda/css/testfile')
    vdo.persistence = Persistence.LONG_TERM
    print('Retention time: {} s'.format(vdo.persistence))
    
    task = Task(command='echo')
    task.scheduler = Scheduler.SLURM
    task.scheduler_opts = {'nodes': 1, 'walltime': '00:05:00', 'queue': 'debug'}
    task.inputs = ['hello', 'world', vdo]
    vds.add(task)
    vds.add(task)
    #task.inputs = [vdo]

    #madats.plan(vds)
    madats.manage(vds)
    #madats.destroy(vds)

if __name__ == '__main__':
    main()
