**************************************************************************
MaDaTS: Managing Data on Tiered Storage for Scientific Workflows

* Author: Devarshi Ghoshal
* v1.1.0
* Created: Oct 25, 2016
* Updated: Feb 19, 2018
**************************************************************************

----------------------------------------------------------------

-![MaDaTS Architecture](docs/figs/madats.png)

MaDaTS provides an integrated data management and workflow execution
framework on multi-tiered storage systems. Users of MaDaTS can execute
a workflow by either specifying the workflow stages in a YAML description
file, or use the API to manage the workflow through VDS. Some examples of
specifying the workflow description and using the API are provided in the
examples/ directory.

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

TEST
-----
To test MaDaTS, do:

        py.test tests/test_madats.py


Example
-------
In order to manage data and workflow, users need to create virtual data objects
and tasks, and add them to a Virtual Data Space (VDS). Data and tasks of the
workflow are managed through MaDaTS by simply calling the `manage` function.
A simple example is provided below. The examples/ directory contains some more
examples that describe the different features of MaDaTS.

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
