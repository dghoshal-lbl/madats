from abstractions.system_interfaces import AbstractExecution
import tigres
from madats.core.task import Task, DataTask
import time
from utils import dagman


class MadatsExecution(AbstractExecution):
    WAIT_TIME = 5

    def wait(self, exec_id):
        pass

    def status(self, exec_id):
        return 0

    def execute(self, dag, async_mode, **kwargs):
        if 'execution' in kwargs:
            execution = kwargs['execution']
        else:
            execution = 'EXECUTION_LOCAL_THREAD'
        execution_name = {
            'EXECUTION_LOCAL_THREAD': "thread",
            'EXECUTION_LOCAL_PROCESS': "process",
            'EXECUTION_DISTRIBUTE_PROCESS': "distribute",
            'EXECUTION_SGE': 'sge'
            }[execution]
        execution_plugin = tigres.utils.Execution.get(execution)        

        task_bins = dagman.bin_execution_order(dag)
        
        tigres.start(name="TigresWF-{}".format(execution_name),
                     log_dest="TigresWF-{}.log".format(execution_name),
                     execution=execution_plugin)

        for tasks in task_bins:
            if len(tasks) == 1:
                self.__execute_single__(tasks[0])
            else:
                self.__execute_parallel__(tasks)
        
        tigres.end()


    def __manage_data_tasks__(self, data_task):        
        print('Waiting for data-task {}'.format(data_task.__id__))
        print("{} {}".format(data_task.command, data_task.params))
        print('Data-task {} completed'.format(data_task.__id__))


    def __execute_single__(self, task):
        task_array = tigres.TaskArray('Single Task')
        input_array = tigres.InputArray('Single Task Inputs')

        task_id = str(task.__id__)

        # data tasks will be handled by the storage system and hence, only monitor them
        if task.type == Task.DATA:
            ttask = tigres.Task("{}".format(task.__id__), tigres.FUNCTION, impl_name=self.__manage_data_tasks__, input_types=[DataTask])
            task_array.append(ttask)
            input_array.append([task])
        else:
            params = []
            if task.params != None:
                params = task.params
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
        else:
            print('Successfully completed task-{}'.format(task.__id__))
    

    def __execute_parallel__(self, tasks):
        task_array = tigres.TaskArray('Parallel Task')
        input_array = tigres.InputArray('Parallel Task Inputs')

        task_ids = []

        for task in tasks:
            # data tasks will be handled by the storage system and hence, only monitor them
            task_id = str(task.__id__)
            task_ids.append(task_id)
            if task.type == Task.DATA:
                ttask = tigres.Task("{}".format(task.__id__), tigres.FUNCTION, impl_name=self.__manage_data_tasks__, input_types=[DataTask])
                task_array.append(ttask)
                input_array.append([task])
            else:
                params = []
                if task.params != None:
                    params = task.params
                ttask = tigres.Task("{}".format(task.__id__), tigres.EXECUTABLE, impl_name=task.command)
                task_array.append(ttask)
                input_array.append(params)

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
        else:
            task_ids = [str(task.__id__) for task in tasks]
            print('Successfully completed tasks-{}'.format(','.join(task_ids)))



