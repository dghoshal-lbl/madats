from madats.core.task import Task
from abstractions.data_interfaces import AbstractWorkflow

class MadatsWorkflow(AbstractWorkflow):
    def parse(self, workflow, type=None, **kwargs):
        tasks = []
        for t in workflow:
            task = Task()
            task_info = workflow[t]
            task.command = task_info['command']
            task.params = task_info['params']
            task.inputs = task_info['inputs'] # task inputs that need to be mapped to VDS
            task.outputs = task_info['outputs'] # task outputs that need to be mapped to VDS
            task.partition = task_info['partition']
            task.walltime = task_info['walltime']
            task.cpus = task_info['cpus']
            tasks.append(task)
            
        return tasks
