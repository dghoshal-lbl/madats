'''
use tigres to manage/execute tasks
three modes:
  - workflow orchestrates data movement [phase-0 (done in C)]
  - storage system orchestrates data movement [phase-1]
  - deadline based data movement (storage system orchestrated)
'''

import tigres
from core.vds_coordinator import DataManagement, DAGManagement
from core.task import Task, DataTask
from db.loader import DbLoader
from db.monitor import DbMonitor
from core.vds import VirtualDataSpace, VirtualDataObject
import time

'''
- translates tasks into tigres tasks and inputs
- creates entries for data tasks in the db
- creates data tasks that poll the db to check data move status before executing the associated compute tasks
'''
class ExecutionManager():
    WAIT_TIME = 5

    def __init__(self, execution):
        self.execution_name = {
            'EXECUTION_LOCAL_THREAD': "thread",
            'EXECUTION_LOCAL_PROCESS': "process",
            'EXECUTION_DISTRIBUTE_PROCESS': "distribute",
            'EXECUTION_SGE': 'sge'
            }[execution]
        self.execution_plugin = tigres.utils.Execution.get(execution)        
        

    def manage_data_tasks(self, data_task_id):        
        print('Waiting for data-task {}'.format(data_task_id))
        db_monitor = DbMonitor(collection='tasks')
        status = 'PENDING'
        id = str(data_task_id)
        while status != 'COMPLETED':
            status = db_monitor.check_task_status(id)
            time.sleep(self.WAIT_TIME)
        print('Data-task {} completed'.format(id))


    def execute(self, dag):
        db_loader = DbLoader(collection='tasks')
        workflow_id = db_loader.insert(dag)

        dag_mgmt = DAGManagement(dag)
        task_bins = dag_mgmt.bin_execution_order()
        
        tigres.start(name="TigresWF-{}-{}".format(workflow_id, self.execution_name),
                     log_dest="TigresWF-{}-{}.log".format(workflow_id, self.execution_name),
                     execution=self.execution_plugin)

        for tasks in task_bins:
            if len(tasks) == 1:
                self.__execute_single__(tasks[0])
            else:
                self.__execute_parallel__(tasks)
        
        
        tigres.end()


    def __execute_single__(self, task):
        task_array = tigres.TaskArray('Single Task')
        input_array = tigres.InputArray('Single Task Inputs')

        db_loader = DbLoader(collection='tasks')
        task_id = str(task.__id__)

        # data tasks will be handled by the storage system and hence, only monitor them
        if task.type == Task.DATA:
            db_loader.update_status(task_id, 'PENDING')
            ttask = tigres.Task("{}".format(task.__id__), tigres.FUNCTION, impl_name=self.manage_data_tasks, input_types=[str])
            task_array.append(ttask)
            input_array.append([task.__id__])
        else:
            db_loader.update_status(task_id, 'RUNNING')
            params = []
            if task.params != None:
                params = task.get_remapped_params()
            ttask = tigres.Task("{}".format(task.__id__), tigres.EXECUTABLE, impl_name=task.command)
            task_array.append(ttask)
            input_array.append(params)

        return_code = 0
        try:
            output = tigres.sequence('single_task', task_array, input_array)
            print(output)
        except tigres.utils.TigresException as e:
            return_code = 1

        if return_code == 1:
            task_check = tigres.check('task', state=tigres.utils.State.FAIL)
            if task_check:
                print("Failed Single!!")
                for task_record in task_check:
                    print(".. Task Failed: {}, {}".format(task_record.name, task_record.errmsg))
            db_loader.update_status(task_id, 'FAILED')
        else:
            print('Successfully completed task-{}'.format(task.__id__))
            db_loader.update_status(task_id, 'COMPLETED')
    

    def __execute_parallel__(self, tasks):
        task_array = tigres.TaskArray('Parallel Task')
        input_array = tigres.InputArray('Parallel Task Inputs')

        db_loader = DbLoader(collection='tasks')
        task_ids = []

        for task in tasks:
            # data tasks will be handled by the storage system and hence, only monitor them
            task_id = str(task.__id__)
            task_ids.append(task_id)
            if task.type == Task.DATA:
                db_loader.update_status(task_id, 'PENDING')
                ttask = tigres.Task("{}".format(task.__id__), tigres.FUNCTION, impl_name=self.manage_data_tasks, input_types=[str])
                task_array.append(ttask)
                input_array.append([task.__id__])
            else:
                params = []
                if task.params != None:
                    params = task.get_remapped_params()
                ttask = tigres.Task("{}".format(task.__id__), tigres.EXECUTABLE, impl_name=task.command)
                task_array.append(ttask)
                input_array.append(params)

        for task_id in task_ids:
            db_loader.update_status(task_id, 'RUNNING')

        return_code = 0
        try:
            output = tigres.parallel('parallel_task', task_array, input_array)
            print(','.join(str(output)))
        except tigres.utils.TigresException as e:
            return_code = 1

        if return_code == 1:
            task_check = tigres.check('task', state=tigres.utils.State.FAIL)
            if task_check:
                print("Failed Parallel!!")
                for task_record in task_check:
                    print(".. Task Failed: {}, {}".format(task_record.name, task_record.errmsg))
            for task_id in task_ids:
                db_loader.update_status(task_id, 'FAILED')
        else:
            task_ids = [str(task.__id__) for task in tasks]
            print('Successfully completed tasks-{}'.format(','.join(task_ids)))
            for task_id in task_ids:
                db_loader.update_status(task_id, 'COMPLETED')
