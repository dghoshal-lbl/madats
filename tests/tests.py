from core.vds_coordinator import DataManagement, DAGManagement
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
    
    data_mgmt = DataManagement(vds)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'burst')
    data_mgmt.create_data_task(vdo1, vdo3)

    vdo4 = vds.copy(vdo1, 'burst')
    data_mgmt.create_data_task(vdo1, vdo4)

    dag = data_mgmt.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)

def test2():
    vds = VirtualDataSpace()

    task1 = Task()
    task1.command = 'echo hello world'
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    data_mgmt = DataManagement(vds)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    
    dag = data_mgmt.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)

def test3():
    vds = VirtualDataSpace()

    task1 = Task()
    task1.command = 'echo hello world'
    data1 = '/scratch/testdir/indata'
    data2 = '/scratch/testdir/outdata'
    
    data_mgmt = DataManagement(vds)
    vdo1 = data_mgmt.create_vdo(data1)
    vdo1.consumers = [task1]
    vdo2 = data_mgmt.create_vdo(data2)
    vdo2.producers = [task1]
    
    vdo3 = vds.copy(vdo1, 'css')
    data_mgmt.create_data_task(vdo1, vdo3, deadline='12:30:00', bandwidth='1000', persist=True)

    dag = data_mgmt.create_dag()

    exe = ExecutionManager('EXECUTION_LOCAL_THREAD')
    exe.execute(dag)
    
if __name__ == '__main__':
    test3()

