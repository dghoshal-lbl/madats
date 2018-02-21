**************************************************************************
MaDaTS: Managing Data on Tiered Storage for Scientific Workflows

* Author: Devarshi Ghoshal
* v1.1.1
* Created: Oct 25, 2016
* Updated: Feb 21, 2018
**************************************************************************

MaDaTS provides an integrated data management and workflow execution
framework on multi-tiered storage systems. Users of MaDaTS can execute
a workflow by either specifying the workflow stages in a YAML description
file, or use the API to manage workflows and associated data. Some examples
of specifying the workflow description and using the API are provided in
the examples/ directory.

The MaDaTS API provides simple data abstractions for managing workflow and
data on multi-tiered storage. It takes a data-driven approach to executing
workflows, where a workflow is mapped to a Virtual Data Space (VDS) consisting
of virtual data objects. A user simply creates a VDS and adds virtual data
objects to the VDS, and MaDaTS takes care of all the necessary data movements
and bindings to seamlessly manage a workflow and associated data across multiple
storage tiers.  

PRE-REQUISITES
--------------
* Python (>= 2.7)
* pip (>= 9.0)

BUILD
-----
To install MaDaTS, do:

        pip install -r requirements.txt
        python setup.py install

The environment variable `MADATS_HOME` should be set prior to using MaDaTS.
The setup script creates a MADATS_HOME file that can be sourced prior to
using MaDaTS to set the environment variable `MADATS_HOME`.

TEST
-----
To test MaDaTS, do:

        source MADATS_HOME
        py.test tests/test_madats.py


Example
-------
In order to manage data and workflow, users need to create virtual data objects
and tasks, and add them to a Virtual Data Space (VDS). Data and tasks of the
workflow are managed through MaDaTS by simply calling the `manage` function.
An example listing the steps to manage a workflow and its data through MaDaTS
is given below.

	import madats
	
	# Create a Virtual Data Space (VDS)
	vds = madats.VirtualDataSpace()

	# Create Virtual Data Object
	vdo = madats.VirtualDataObject('/path/to/data')

	# Create a Task
	task = madats.Task(command='/application/program')
	task.params = ['arg1', 'arg2', vdo]

	# Associate tasks to virtual data objects
	vdo.producers = [task]

	# Add the virtual data object to the VDS
	vds.add(vdo)

	# Manage data and workflow execution through MaDaTS
	madats.manage(vds)

It is important to note how MaDaTS uses data as the first-class
citizen. Everything in MaDaTS is centered around virtual data objects.
Tasks are specified as *producers* and *consumers* of virtual data
objects. A VDS is a collection of several virtual data objects that
that represent the data of a workflow.

In addition to creating a VDS step-by-step as shown above, users
can also map a workflow into VDS. MaDaTS provides the `map` function
that takes as input a YAML description of a workflow, or a dict-like
object (similar to JSON). 

       import madats

       # Map a YAML workflow description to VDS
       vds = madats.map('workflow/description/yaml', language='yaml') 

$MADATS_HOME/examples/madats_workflow.yaml specifies a description file
for a sample workflow. The examples/ directory also contains examples that
describe different ways of specifying a workflow and data management properties
in MaDaTS.

Data Management Abstractions
----------------------------
MaDaTS provides a simple data management abstraction through the **manage** interface. Users simply
create a VDS and tell MaDaTS to manage workflow data and tasks. The 'manage' interface
also allows users to group the tasks in different ways. Additionally, users can also
select the data management strategy in MaDaTS. By default, MaDaTS provides three data
management strategies - i) *workflow-aware*: data management decisions are made based
on the structure of the workflow, ii) *storage-aware*: data management decisions are
made based on the storage properties, and iii) *passive*: allows users to define custom
data placement and movement policies. The examples/ directory contains some examples
of defining data and workflow management strategies in MaDaTS.

Users can also define new data management strategies in MaDaTS by defining data tasks
through the **create_data_task** interface. This adds the necessary data operations
needed for efficient execution of the workflow. Both *workflow-aware* and *storage-aware*
data management strategies use this interface to define data operations based on
respective policies.

Configuring Storage Tiers
--------------------------
MaDaTS is designed to manage data seamlessly across multiple storage tiers. The storage
configuration can be defined through `config/storage.yaml`. The configuration file contains
an identifier for each storage tier and its associated properties.

Batch Scheduler
---------------
MaDaTS currently supports PBS and SLURM batch schedulers for managing workflow tasks as
batch jobs. The various options for the batch schedulers are specified in their respective
configuration files `config/pbs.cfg` and `config/slurm.cfg`. Users can also add their own
batch schedulers by specifying the respective configuration files.