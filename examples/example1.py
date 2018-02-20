'''
Example-1: 
     - Specify data management policies in Madats 
     - Describe the workflow as a Dictionary Object
'''
from random import randint
import os
import madats

def example():
    workflow = {}
    num_tasks = randint(2, 5)
    scratch_dir = '/scratch/scratchdirs/cscratch1'
    for i in range(num_tasks):
        task = 'task' + str(i)
        workflow[task] = {}
        workflow[task]['vin'] = [os.path.join(scratch_dir, 'data') + str(i-1)]
        workflow[task]['vout'] = [os.path.join(scratch_dir, 'data') + str(i)]
        workflow[task]['params'] = [os.path.join(scratch_dir, 'data') + str(i-1),
                                    os.path.join(scratch_dir, 'data') + str(i)]
        workflow[task]['command'] = 'echo ' + str(i)
        workflow[task]['scheduler'] = 'slurm'
        workflow[task]['scheduler_opts'] = {}
        workflow[task]['scheduler_opts']['queue'] = 'debug'
        workflow[task]['scheduler_opts']['walltime'] = '00:30:00'
        workflow[task]['scheduler_opts']['cpus'] = 2

    policies = [madats.Policy.NONE, madats.Policy.WORKFLOW_AWARE, madats.Policy.STORAGE_AWARE]
    for policy in policies:
        print("****** Policy: {} ******".format(madats.Policy.name(policy)))
        vds = madats.map(workflow, language='DictObj', policy=policy)
        madats.manage(vds)
    
if __name__ == '__main__':
    example()

