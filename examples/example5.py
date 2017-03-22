from random import randint
import madats

def example():
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
    example()

