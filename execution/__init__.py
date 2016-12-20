'''
workflow_managers: contain code for workflow managers that can be used as standalone solution
batch schedulers: contain code to generate scripts for managing workflows only using batch schedulers
hybrid: merging workflow_managers, batch schedulers, and storage systems

- The task/bin information is inserted into a db (preferably, mongodb)
- All the tasks/bins are extracted by the execution manager
- If a workflow manager is to be used, then pass the tasks/bins to the workflow manager
- If a batch scheduler is to be used, then pass the tasks/bins to the batch scheduler
- If a hybrid approach is to be used, then use a combination of workflow managers, scheduler and storage
OR
make this one implementation of VDS, where execution manager inserts tasks/bins to a db
and there are separate components that manage the execution of different tasks/bins
- current implementation: task/bin execution => tigres; workload management => slurm
[additionally, data task management can be storage system]

Fields in the db:
workflow id, workflow name, task id, task name, task type (compute/data)
task command, task params, task config,
submission time, wait time, run time, deadline (in seconds)
dirty (has the task deadline changed), status (pending/processing/completed/failed)
'''
