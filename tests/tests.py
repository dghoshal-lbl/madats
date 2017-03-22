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

    policies = ['WFA', 'STA', 'PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.create()
        madats.map(vds, workflow)
        madats.plan(vds, policy)
        madats.manage(vds)
        madats.destroy(vds)

def test1():
    workflow = {}
    num_tasks = randint(2, 5)
    for i in range(num_tasks):
        task = 'task' + str(i)
        workflow[task] = {}
        workflow[task]['inputs'] = ['/scratch/data' + str(i-1)]
        workflow[task]['outputs'] = ['/scratch/data' + str(i)]
        workflow[task]['params'] = ['/scratch/data' + str(i-1), '/scratch/data' + str(i)]
        workflow[task]['command'] = 'echo ' + str(i)
        workflow[task]['partition'] = 'debug'
        workflow[task]['walltime'] = '00:30:00'
        workflow[task]['cpus'] = 2

    policies = ['PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.create()
        madats.map(vds, workflow)
        madats.plan(vds, policy)
        madats.manage(vds)
        madats.destroy(vds)

def test2():
    workflow = {}
    num_tasks = randint(2, 5)
    for i in range(num_tasks):
        task = 'task' + str(i)
        workflow[task] = {}
        workflow[task]['inputs'] = ['/scratch/data' + str(i-1)]
        workflow[task]['outputs'] = ['/scratch/data' + str(i)]
        workflow[task]['params'] = ['/scratch/data' + str(i-1), '/scratch/data' + str(i)]
        workflow[task]['command'] = 'echo ' + str(i)
        workflow[task]['partition'] = 'debug'
        workflow[task]['walltime'] = '00:30:00'
        workflow[task]['cpus'] = 2

    workflow['task0']['inputs'] = ['A:/home/dghoshal/data-1'] 
    workflow['task0']['params'] = ['A:/home/dghoshal/data-1', '/scratch/data0']
    #workflow['task0']['inputs'] = ['/archive/data-1'] 
    #workflow['task0']['params'] = ['/archive/data-1', '/scratch/data0']
    dataprops = {'/scratch/data0': {'persist': True}}
    policies = ['PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.create()
        madats.map(vds, workflow, dataprops)
        madats.plan(vds, policy)
        #madats.manage(vds, scheduler='slurm', auto_exec=False)
        #madats.manage(vds, auto_exec=False)
        madats.manage(vds)
        madats.destroy(vds)

def test3():
    policies = ['PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.create()
        vdo1 = madats.vds.VirtualDataObject('/scratch/data0')
        vdo2 = madats.vds.VirtualDataObject('/scratch/data1')
        vdo3 = madats.vds.VirtualDataObject('/scratch/data2')
        task1 = madats.vds.Task()
        task1.command = 'echo task0'
        ## either use filesystem data objects
        task1.add_param('/scratch/data0')
        task1.add_param('/scratch/data1')
        task2 = madats.vds.Task()
        task2.command = 'echo'
        ## or use virtual data objects
        task2.add_param('task1')
        task2.add_param(vdo2)
        task2.add_param(vdo3)        
        vdo1.add_consumer(task1)
        vdo2.add_producer(task1)
        vdo2.add_consumer(task2)
        vdo3.add_producer(task2)
        vdo2.persist(True)
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        madats.plan(vds, policy)
        madats.manage(vds)
        madats.destroy(vds)


def test4():
    policies = ['PASSIVE']
    for policy in policies:
        print("****** Policy: {} ******".format(policy))
        vds = madats.create()
        vdo1 = madats.vds.VirtualDataObject('/scratch/data0')
        vdo2 = madats.vds.VirtualDataObject('/scratch/data1')
        vdo3 = madats.vds.VirtualDataObject('/scratch/data2')
        task1 = madats.vds.Task()
        task1.command = 'echo task0'
        ## either use filesystem data objects that are mapped to VDS
        task1.add_param('/scratch/data0')
        task1.add_param('/scratch/data1')
        task2 = madats.vds.Task()
        task2.command = 'echo'
        ## or use virtual data objects
        task2.add_param('task1')
        task2.add_param(vdo2)
        task2.add_param(vdo3)        
        vdo1.add_consumer(task1)
        vdo2.add_producer(task1)
        vdo2.add_consumer(task2)
        vdo3.add_producer(task2)
        vdo2.persist(True)
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        madats.plan(vds, policy)
        #madats.manage(vds, scheduler='slurm', auto_exec=False)
        madats.manage(vds, scheduler='slurm')
        madats.destroy(vds)
    
if __name__ == '__main__':
    test2()

