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

1.  Command-line utilities
    madats -w <workflow-description> [-l <workflow language> -m <execution mode> -p <data management policy>]
    madats-copy <source> <dest> [-p <protocol> --remove_src]
    madats-gen -i <workflow-desc> [-f <format (yaml/json)> -o <outpath>] 
    madats-execute -w <workflow> -e <workflow-engine (slurm/pbs/tigres)>
    madats-info --list-workflows/--stats <workflow-id>/--query <query>

2. API
   madats.initVDS()
   madats.map(workflow, language, policy)
   madats.plan(vds)
   madats.manage(vds, execute_mode)
   madats.query(vds, query)
   madats.destroy(vds)

Example
-------
	import madats
	
	# Initialize a Virtual Data Space
	vds = madats.initVDS()
	# Set up the data management policy for the VDS
	vds.data_management_policy = madats.Policy.STORAGE_AWARE

	# Create VDOs and associate them with tasks in the VDS
	vdo = vds.create_vdo('/path/to/data')
	task = vds.create_task(command='/application/program')
	task.inputs = ['constant_param', vdo]

	# Plan workflow execution and data management 
	madats.plan(vds)
	# Manage data and workflow execution based on the plan 
	madats.manage(vds, madats.ExecutionMode.DAG)

	# Cleanup
	madats.destroy(vds)

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

Usage
--------
1. Using the MaDaTS API
   - compose a data-centric workflow using MaDaTS API    
     #-- create a VDS [madats.create()]  
     -- create a VDS [madats.initVDS()]  
     -- add VDOs to a VDS [vds.add(vdo)]  
     -- add producers and consumers to VDOs [vdo.add_producers(task)/consumers(task)]  
     #-- plan the data management strategies [madats.plan(vds, policy)]  
     -- manage the data and execute the workflow [madats.manage(vds)]  
     -- destroy the workflow [madats.destroy(vds)]  

2. Using a workflow description  
   - provide a description of the workflow [madats.map(vds, workflow)]  
   - select the appropriate data management strategy [madats.plan(vds, policy)]  
   - generate and execute batch scripts [madats.manage(vds, scheduler)]  

Test
--------
	 python tests/tests.py

