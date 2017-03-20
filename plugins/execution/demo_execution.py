from abstractions.system_interfaces import AbstractExecution
import tigres
import time


class DemoExecution(AbstractExecution):
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
        
        print('--------------------------------------')
        for k in dag:
            task = k.command
            children = [c.command for c in dag[k]]
            print("{}(T:{})[{}]: {}".format(task, k.type, k.params, children))
        print('--------------------------------------')
        return 0


if __name__ == '__main__':
    exe = DemoExecution()
    dag = {}
    exe.execute(dag, False)
