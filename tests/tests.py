from random import randint
import madats

def test0():
    workflow = {}
    num_tasks = randint(2, 5)
    for i in range(num_tasks):
        task = 'task' + str(i)
        workflow[task] = {}
        workflow[task]['inputs'] = ['/scratch/data' + str(i-1)]
        workflow[task]['outputs'] = ['/scratch/data' + str(i)]
        workflow[task]['params'] = ['/scratch/data' + str(i-1), '/scratch/data' + str(i)]
        workflow[task]['command'] = 'cmd' + str(i)
        workflow[task]['partition'] = 'debug'
        workflow[task]['walltime'] = '00:30:00'
        workflow[task]['cpus'] = 2

    vds = madats.create(workflow)
    madats.plan(vds)
    madats.manage(vds)
    madats.destroy(vds)
    
if __name__ == '__main__':
    test0()

