MaDaTS: Managing Data on Tiered Storage for Scientific Workflows
----------------------------------------------------------------

-![MaDaTS Architecture](docs/figs/madats.png)

MaDaTS provides an integrated data management and workflow execution
framework. It provides high-level data abstractions through the Virtual
Data Space (VDS). VDS simplifies the data management across multiple
storage tiers and file systems during the lifetime of workflow execution.

In addition to the workflow descriptions, users can programmatically
create and manage a VDS, which is a data-centric way of representing
the workflow and data. The current API supports Python. Below are the
key functions in the API.

The high-level madats functions allow users to manage workflow data
and tasks on multi-tiered storage hierarchy through a VDS.

    	madats.VirtualDataSpace()  : creates a VDS

        madats.VirtualDataObject() : creates a VDO

        madats.Task()		   : creates a task to which the VDO can be associated

        madats.manage()		   : manages a VDS


VDS provides the following functions to create, modify and delete
virtual data objects and tasks onto VDS.

        vds.add()	: adds a task or a VDO

        vds.copy()	: copies a VDO to another VDO

        vds.replace()	: replaces a VDO from another VDO

        vds.delete()	: deletes a task or a VDO from the VDS

        vds.map()	: maps a datapath to a VDO on VDS

	vds.create_data_task() : creates a data task that moves data between tiers


Virtual Data Object is a data abstraction for managing data on multi-tiered
storage hierarchy.

For defining associations:

        vdo.add_producer()

        vdo.add_consumer()

Attributes:

        vdo.size
        vdo.persistence
        vdo.replication
        vdo.deadline
        vdo.qos


A task and a data-task in a VDS defines an action on one or more VDOs. 

Example
-------
	import madats
	
	# Create a Virtual Data Space
	vds = madats.VirtualDataSpace()
	# Set up the data management policy for the VDS
	vds.data_management_policy = madats.Policy.STORAGE_AWARE

	# Create VDOs and associate them with tasks in the VDS
	vdo = madats.VirtualDataObject('/path/to/data')
	task = madats.Task(command='/application/program')
	task.inputs = ['constant_param', vdo]

	# Add the task
	vds.add(task)

	# Manage data and workflow execution based on the plan 
	madats.manage(vds, madats.ExecutionMode.DAG)

Install
--------
1. Using Anaconda python  
      	 conda create -n <env> python   
	 source activate <env>  
	 python setup.py install

2. Using virtualenv  
    	 virtualenv <venv>  
    	 source <venv>/bin/activate    
    	 python setup.py install  
