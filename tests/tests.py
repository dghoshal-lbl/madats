from core.vds_coordinator import Coordinator
from core.task import Task, DataTask
from db.loader import DbLoader
from db.monitor import DbMonitor
from core.vds import VirtualDataSpace, VirtualDataObject
from execution.execution_manager import ExecutionManager
import time

def test1():
    vds = VirtualDataSpace()

    task1 = Task()
    task1.command = 'ls -lrt'
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    coordinator = Coordinator(vds)
    vdo1 = coordinator.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = coordinator.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'burst')
    coordinator.create_data_task(vdo1, vdo3)

    vdo4 = vds.copy(vdo1, 'burst')
    coordinator.create_data_task(vdo1, vdo4)

    dag = coordinator.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)

def test2():
    vds = VirtualDataSpace()

    task1 = Task()
    task1.command = 'echo hello world'
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    coordinator = Coordinator(vds)
    vdo1 = coordinator.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = coordinator.create_vdo(data2)
    vdo2.producers = [task1]
    
    dag = coordinator.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)

def test3():
    vds = VirtualDataSpace()

    task1 = Task()
    task1.command = 'echo hello world'
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    coordinator = Coordinator(vds)
    vdo1 = coordinator.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = coordinator.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'css')
    coordinator.create_data_task(vdo1, vdo3, deadline='12:30:00', bandwidth='1000', persist=True)

    dag = coordinator.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)

def test4():
    vds = VirtualDataSpace()

    task0 = Task()
    task0.command = '/bin/bash /Users/DGhoshal/csswf/stage1.sh'
    task1 = Task()
    task1.command = '/bin/bash /Users/DGhoshal/csswf/stage2.sh'

    data0 = '/scratch/testdir/indata0'
    data1 = '/scratch/testdir/indata1'
    data2 = '/scratch/testdir/tmpdata'
    data3 = '/scratch/testdir/outdata'
    
    coordinator = Coordinator(vds)
    vdo0 = coordinator.create_vdo(data0)
    vdo0.consumers = [task0]
    vdo1 = coordinator.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = coordinator.create_vdo(data2)
    vdo2.producers = [task0]
    vdo2.consumers = [task1]
    vdo3 = coordinator.create_vdo(data3)
    vdo3.producers = [task1]

    task0.params = ['-i', vdo0, '-o', vdo2]
    task1.params = ['-i', vdo1, vdo2, '-o', vdo3]
    
    dag = coordinator.create_dag()
    task_order = coordinator.batch_execution_order()
    i = 0
    print('------------------------')
    print('Default workflow stages')
    print('------------------------')    
    for task in task_order:
        print("Stage-{}: {} {}".format(i, task.command, task.get_remapped_params()))
        i += 1
    print('------------------------')

    vdo0_1 = vds.copy(vdo0, 'css')
    vdo1_1 = vds.copy(vdo1, 'css')
    vdo2_1 = vds.copy(vdo2, 'css')
    vdo3_1 = vds.copy(vdo3, 'css')

    coordinator.create_data_task(vdo0, vdo0_1, deadline='12:30:00', bandwidth='1000')
    coordinator.create_data_task(vdo1, vdo1_1, deadline='12:35:00', bandwidth='100')
    coordinator.create_data_task(vdo2, vdo2_1, bandwidth='1000', persist=False)
    coordinator.create_data_task(vdo3_1, vdo3, bandwidth='1000', persist=True)

    dag = coordinator.create_dag()

    print('----------------------')
    task_bins = coordinator.bin_execution_order()
    i = 0
    print('----------------------')
    print('Task Bins')
    print('----------------------')
    for tasks in task_bins:
        for task in tasks:
            print("Bin{}: {} {}".format(i, task.command, task.get_remapped_params()))
        i += 1
    print('----------------------')

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)
    
if __name__ == '__main__':
    test4()

