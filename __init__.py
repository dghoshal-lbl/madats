import uuid
import storage.storage_manager as storage_manager
from core.vds import VirtualDataSpace, VirtualDataObject
from core.vds_coordinator import DataManagement
from core.task import Task, DataTask

if __name__ == '__main__':
    vds = VirtualDataSpace()
    task_map = {}

    task1 = Task(0)
    task1.set_command('./hello_world')
    task_map[0] = task1
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    data_mgmt = DataManagement(vds, task_map)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'burst')
    data_mgmt.create_data_task(vdo1, vdo3)

    for k in task_map:
        task = task_map[k]
        print("Task ({}): {} {}".format(task.__id__, task.type, task.command))

    vdo_list = vds.get_vdo_list()
    for vdo in vdo_list:
        print("VDO: {}".format(vdo.__id__))
    #dag_mgmt = DAGManagement(vds, dag, task_map)
