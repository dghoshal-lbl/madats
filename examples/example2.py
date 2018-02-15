from random import randint
import madats

def example():
    workflow = {}
    num_tasks = randint(2, 5)
    for i in range(num_tasks):
        task = 'task' + str(i)
        workflow[task] = {}
        workflow[task]['vin'] = ['/scratch/data' + str(i-1)]
        workflow[task]['vout'] = ['/scratch/data' + str(i)]
        workflow[task]['params'] = ['/scratch/data' + str(i-1), '/scratch/data' + str(i)]
        workflow[task]['command'] = 'echo ' + str(i)
        workflow[task]['partition'] = 'debug'
        workflow[task]['walltime'] = '00:30:00'
        workflow[task]['cpus'] = 2

    print("Num tasks: {}".format(len(workflow)))
    policies = ['PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.map(workflow, language='DictObj')
        #madats.map(vds, workflow)
        madats.manage(vds)
    
if __name__ == '__main__':
    example()

