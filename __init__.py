import uuid
import storage.storage_manager as storage_manager
from core.vds import VirtualDataSpace, VirtualDataObject
from core.vds_coordinator import DataManagement, DAGManagement
from core.task import Task, DataTask
from execution.db_loader import DbLoader
from execution.db_monitor import DbMonitor

if __name__ == '__main__':
    vds = VirtualDataSpace()
    task_map = {}

    task1 = Task()
    task1.set_command('./hello_world')
    task_map[task1.__id__] = task1
    task2 = Task()
    task2.set_command('ls -lrt')
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    data3 = '/scratch/testdir/indata2'
    data4 = '/dw/testdir/outdata2'

    data_mgmt = DataManagement(vds)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    vdo2.consumers = [task2]
    vdo2.persist = False
    vdo6 = data_mgmt.create_vdo(data3)
    vdo6.consumers = [task2]
    #vdo8 = data_mgmt.create_vdo(data4)
    #vdo8.producers = [task2]

    vdo3 = vds.copy(vdo1, 'burst')
    data_mgmt.create_data_task(vdo1, vdo3)

    #vdo4 = vds.copy(vdo8, 'scratch')
    #data_mgmt.create_data_task(vdo8, vdo4)

    vdo5 = vds.copy(vdo2, 'burst')
    data_mgmt.create_data_task(vdo2, vdo5)
    #vds.delete(vdo2)
    vdo7 = vds.copy(vdo6, 'burst')
    data_mgmt.create_data_task(vdo7, vdo6)
#    vdo_list = vds.get_vdo_list()
#    for vdo in vdo_list:
#        print("VDO: {}".format(vdo.__id__))

#    print("Extended DAG of the Workflow...")
    dag = data_mgmt.create_dag()
#    print(dag)

    print('----------------------')
    dag_mgmt = DAGManagement(dag)
    for t in dag:
        succ = [str(s.command)+' '+str(s.params) for s in dag_mgmt.successors(t)]
        print("{} {}: {}".format(t.command, t.params, succ))

    '''
    for task in dag:
        preds = dag_mgmt.predecessors(task)
        pids = [str(p.__id__) for p in preds]
        print("Pred({}): {}".format(task.__id__, ','.join(pids)))

    for task in dag:
        succs = dag_mgmt.successors(task)
        sids = [str(p.__id__) for p in succs]
        print("Succ({}): {}".format(task.__id__, ','.join(sids)))
    '''

    print('----------------------')
    print('Task execution order')
    print('----------------------')
    task_order = dag_mgmt.batch_execution_order()
    for task in task_order:
        print("{} {}".format(task.command, task.params))

    db_loader = DbLoader()
    db_loader.truncate()
    wf_id = db_loader.insert(dag)
    wf_id = db_loader.insert_data_tasks(dag)

    db_monitor = DbMonitor()
    db_monitor.query()
    task_id = task_order[0].__id__
    res = db_monitor.check_task_status(str(task_id))
#    print(res)

    task_bins = dag_mgmt.bin_execution_order()
    i = 0
#    print(dag)
    print('----------------------')
    print('Task Bins')
    print('----------------------')
    for tasks in task_bins:
        for task in tasks:
            print("Bin{}: {} {}".format(i, task.command, task.params))
        i += 1
