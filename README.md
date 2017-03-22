MaDaTS: Managing Data on Tiered Storage for Scientific Workflows
----------------------------------------------------------------

-![MaDaTS Architecture](docs/figs/madats.png)

MaDaTS provides an integrated data management and workflow execution
framework. It provides high-level data abstractions through a virtual
data space (VDS) for managing data across a multi-tiered storage hierarchy
during the lifetime of workflow execution. Users can programmatically
create and manage VDS, and can also use command-line utilities to manage
workflows and associated data through VDS. It uses a pluggable architecture
as shown in the figure above. Application programmers can build and use
specific plugins to integrate with different workflows and systems. By default,
MaDaTS provides a set of plugins for managing workflows and data on HPC systems.
These plugins use the Tigres workflow API for managing their workflows and
are able to interface with SLURM and PBS scheduler. It can parse workflow
descriptions written in JSON, libconfig and YAML languages.

In addition to the workflow descriptions, users can programmatically
create and manage a VDS, which is a data-centric way of representing
the workflow and data. The current API supports Python. Below are the
key functions in the API.

    1. create a VDS
        madats.create() -> VDS  

    2. add virtual objects to VDS 
    2.1. map a workflow into VDS
        madats.map(VDS, workflow[, data_properties])  
    2.2. create a data-centric workflow in VDS
	madats.vds.VirtualDataObject(data_object) -> VirtualDataObject  
    	madats.vds.Task() > Task  
    	VirtualDataObject.add_consumer(Task)  
    	VirtualDataObject.add_producer(Task)  
    	VDS.add(VirtualDataObject)  

    3. define data management policy and extend workflow  
       madats.plan(VDS, policy)  

    4. manage data and workflow  
       madats.manage(VDS[, async, scheduler, auto_exec])  

    5. destroy the VDS  
       madats.destroy(VDS)  

Dependencies
---------------
   - Tigres (if using MaDaTS execution plugin)

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
     -- create a VDS [madats.create()]  
     -- add VDOs to a VDS [vds.add(vdo)]  
     -- add producers and consumers to VDOs [vdo.add_producers(task)/consumers(task)]  
     -- plan the data management strategies [madats.plan(vds, policy)]  
     -- manage the data and execute the workflow [madats.manage(vds)]  
     -- destroy the workflow [madats.destroy(vds)]  

2. Using a workflow description  
   - provide a description of the workflow [madats.map(vds, workflow)]  
   - select the appropriate data management strategy [madats.plan(vds, policy)]  
   - generate and execute batch scripts [madats.manage(vds, scheduler)]  

Test
--------
	 python tests/tests.py

Plugins
--------
The plugins in MaDaTS can be configured using config/config.ini.
More plugins can be built and kept in the appropriate directory
under plugins/. The plugins should inherit the corresponding
abstract interfaces, the module should be created under the
appropriate plugin-type, and the class name should be in the format:
<Plugin-name><Plugin-type>. For example, the MaDaTS storage plugin
is in the module plugins.storage.madats_storage and the class
name is MadatsStorage(AbstractStorage).

Currently, MaDaTS uses 6 different plugins.

1. Workflow: provides an abstraction for the workflow  
   Module Directory: plugins/workflow  
   Extends: AbstractWorkflow  
   Abstract Functions: parse()  

2. Data-manager: provides an abstraction for data management policies  
   Module Directory: plugins/datamgr  
   Extends: AbstractDatamgr  
   Abstract Functions: policy_engine()  

3. Storage: provides an abstraction for storage interfaces  
   Module Directory: plugins/storage  
   Extends: AbstractStorage  
   Abstract Functions: get_hierarchy(), get_id_path(), get_storage_path(), get_storage_id()  

4. Execution: provides an abstraction for workflow execution engines  
   Module Directory: plugins/execution  
   Extends: AbstractExecution  
   Abstract Functions: execute(), wait(), status()  

5. Copy: provides an abstraction for copying/moving/transferring data  
   Module Directory: plugins/copy  
   Extends: AbstractCopy  
   Abstract Functions: copy(), poll()  

6. Scheduling: provides an abstraction for job schedulers and job management  
   Module Directory: plugins/scheduling  
   Extends: AbstractScheduling  
   Abstract Functions: set(), submit(), wait(), status()  




   