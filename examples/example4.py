'''
Example-4: 
     - Describe the workflow through Madats API by mapping
       datapths to virtual data objects
     - Specify intermediate data to be persistent
     - Update config/storage.yaml with the appropriate storage
       tiers and specify the datadir to make this example work
'''

from madats import VirtualDataSpace, VirtualDataObject, Task
from madats import Persistence
from madats import Policy
import os
import madats

def main():
    datadir = '/scratch/scratchdirs/cscratch1'
    # create a VDS
    vds = VirtualDataSpace()
    vds.strategy = Policy.STORAGE_AWARE
    print('Data management policy: {}'.format(Policy.name(vds.strategy)))

    # create VDOs
    vdo1 = VirtualDataObject(os.path.join(datadir, 'in1'))
    vdo2 = VirtualDataObject(os.path.join(datadir, 'in2'))
    vdo3 = VirtualDataObject(os.path.join(datadir, 'inout1'))
    vdo3.persistence = Persistence.LONG_TERM

    # create tasks
    task = Task(command='cat')
    task.params = [vdo1, vdo2, '>', vdo3]
    task1 = Task(command='cat')
    task1.params = [vdo3]
    
    # define VDO and task associations
    vdo1.consumers = [task]
    vdo2.consumers = [task]
    vdo3.producers = [task]
    vdo3.consumers = [task1]
    vds.add(vdo1)
    vds.add(vdo2)
    vds.add(vdo3)

    # manage VDS
    madats.manage(vds)

if __name__ == '__main__':
    main()
