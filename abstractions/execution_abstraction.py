'''
Execution manager abstraction
'''

import abc

class ExecutionAbstract():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Execution manager abstraction')
        pass

    @abc.abstractmethod
    def execute_data_task(self, data_task, **kwargs):
        #print('Manage data tasks')
        raise NotImplementedError()


    @abc.abstractmethod
    def execute_compute_task(self, compute_task, **kwargs):
        raise NotImplementedError()
        

    @abc.abstractmethod
    def execute_workflow(self, workflow, **kwargs):
        raise NotImplementedError()
