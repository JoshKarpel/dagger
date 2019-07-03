#!/usr/bin/env python

import classad
import htcondor

# Create a DAG object. I'm assuming the first positional argument will still
# support initializing a DAG object from a DAG file. We would like to be able
# to write out the DAG file from the Python object, then read it back in later
# from a different process and submit it. This would also help for re-submitting 
# a DAG file.

### dag = htcondor.Dag(directory="/dag/files/to/be/written/here")
dag = dagger.DAG(
    jobstate_log="jobstate.log",
    config_file="dag.config",
    dot_config=dagger.DotConfig(
        "dag.dot", update=True, overwrite=False, include_file="include.dot"
    ),
    node_status_file=dagger.NodeStatusFile(
        "node_status_file", update_time=50, always_update=True
    ),
)

# In the SDF we currently write out, custom attributes need to use +Name to
# be added to the ClassAd. Here we would use the existing classad module to 
# define the attributes directly, and have htcondor know how to convert the
# ClassAd object to a submit description file (or equivalent).

### jobA = classad.ClassAd()
### jobA['Universe'] = "vanilla"
### jobA['Notification'] = "NEVER"
### jobA['TransferExecutable'] = False
### jobA['AccountingGroup'] = classad.Attribute("DWA_Share")
# MRC: Important to make the distinction here between nodes and jobs. DAGs have
# nodes. Jobs are a subcomponent of nodes.
nodeA = dag.Node(
	name = "A",
	job = {
		"Executable": "/bin/echo",
		"Arguments": "Testing",
		"Universe": "vanilla",
		"Notification": "NEVER",
		"TransferExecutable": "false",
		"AccountingGroup": classad.Attribute("DWA_Share")
	}
)


# This defines a DWA_Share attribute that gets it's value from the VARS section
# of the DAG. If this doesn't work we could switch to +DWA_Share = ... in the
# DAG to add it to the classAd directly. (I think the only reason we don't 
# already do this is that the feature was added after we wrote the DAG/SDF
# writer section.)
# MRC: I'm not really sure what the following line is supposed to do? Are we
# setting a job attribute here or a var? If it's a var, we should probably put
# it directly in the nodeA.vars object.
nodeA['DWA_Share'] = '$(DWA_SHARE)'

# JOB A /sdf/created/for/jobA DIR /tmp
### dag.addJob("A", jobA, dir="/tmp")
# We don't need to explicitly add the job to the dag. This happens automatically
# when it gets instantiated in dag.Node()

### print dag.jobs['A'].classAd
print dag.Nodes['A'].classad
print nodeA.classad
# [ Notification = "NEVER"; Universe = "vanilla"; <...> ]

# Add a post script: SCRIPT POST A /path/to/script/
### dag.jobs['A'].addScript('/path/to/script', type=htcondor.Job.POST)
nodeA.post = dag.Script(
	executable = "/path/to/script",
	arguments = "my args go here",
	retry = True
)

### print dag.jobs['A'].scripts
print dag.Nodes['A'].post
print nodeA.post
### ['SCRIPT POST A /path/to/script/']

# Add custom attributes: VARS A DWA_Share="dragon3_anim"
### dag.jobs['A'].vars = {"DWA_Share": "dragon3_anim",
###                      "DWA_AutoMigrate": False}
dag.Nodes['A'].vars = {
	"DWA_Share": "dragon3_anim",
	"DWA_AutoMigrate": "False"
}


# Append or change vars before writing out file
### dag.jobs['A'].vars['DWA_Share'] = "dragon3_misc"
# MRC: Following up on my previous comment, I think the below is a good syntax
# for changing VARS values after creating the Node.
dag.Nodes['A'].vars['DWA_Share'] = "dragon3_misc"

# Using VARS allows multiple JOB lines to use the same SDF, while having 
# different values for some of the attributes.

# MRC: We don't want to define jobs using classads, we want to define them
# using htcondor.Submit() objects.
### jobBAd = classad.ClassAd()
### jobXAd = classad.ClassAd()
nodeB = dag.Node(
	name = "B",
	job = { "Executable": "/bin/echo", "Arguments": "Testing" }
)
nodeX = dag.Node(
	name = "X",
	job = {	"Executable": "/bin/echo", "Arguments": "Testing" }
)
nodeY = dag.Node(
	name = "Y",
	job = {	"Executable": "/bin/echo", "Arguments": "Testing" }
)
# ...

# Same functionality as above, I just wanted to illustrate that I was picturing
# the Jobs as their own class. This would let them keep track of per-JOB things
# themselves, and have getter functions for them.
### jobB = htcondor.Job(jobBAd)
### jobB.dir = "/tmp"

### dag.jobs['B'] = jobB

### dag.jobs['X'] = htcondor.Job(jobX)
### dag.jobs['Y'] = htcondor.Job(jobX)

### dag.jobs['X'].dir = "/tmp"

# MRC: I'm suggesting a couple of new methods below, addChild() and addParent(),
# to set up dependencies in the graph. I'm not totally sold on this approach.
# I like it better than addDepdency() because it's more clear which direction
# the edges are pointing, 

# PARENT A CHILD X, Y
### dag.addDependency('A', ['X', 'Y'])
nodeA.addChild([nodeX, nodeY])

# PARENT X, Y CHILD B
### dag.addDependency(['X', 'Y'], 'B')
nodeB.addParent([nodeX, nodeY])

print dag.Nodes['A'].getChildren()
# ['X', 'Y']

print dag.Nodes['X'].getParents()
# ['A']

# I could see a dag.getDependencies() being useful, that basically just gets the 
# children and parents for each Job then prints out a simplified version. The 
# Dag object would probably need a way of doing this anyway to be able to print
# the dependency lines to the DAG file.

# Ability to add classAd attributes to the DAGMan job itself. This differentiates
# attributes of the Dag Python object from these ClassAd attributes of the DAGMan
# process that will be created.
### dagManAd = classad.ClassAd()
### dagManAd['DWA_Iteration'] = 1
### dag.classAd = dagManAd
# MRC: To add attributes to the DAGMan job itself, put them in the submit_args
# (which I really should have called dag_args) dict below.
# SET_JOB_ATTR DWA_Iteration = 1

# DOT <DAG dir>/<name> UPDATE
dag.addDot(name="submission.dot", update=True)

# There are a lot of keywords we don't currently use for the DAG file, but 
# presumably the Dag and Job classes would allow using them with either a 
# function or attribute. (Not necessarily both, I just don't have a preference.)

# CONFIG /path/to/config
# dag.config = "/path/to/config" or dag.setConfig("/path/to/config")

# PRIORITY A 50
# jobA.priority = 50 or jobA.setPriority(50)
# PRIORITY ALL_NODES 100
# dag.priority = 100 dag.setPriority(100)

# Create a submit object for the DAG same as if it was initialized from a file.
# Ideally we'd also be able to set all of the same things as attributes of the 
# Dag object itself, but it makes sense to still support this.
submit_args = {
	"maxidle" : 10,
	"maxpost": 5,
	"allowversionmismatch": True,
	# We currently add multiple -append flags from different places, but we
	# would probably switch to whatever new method of adding attributes to the
	# DAGMan job is added to the bindings. I'm not sure how this works for non-
	# DAG submits in the current bindings, but I made append a dictionary here
	# as we can't have multiple "append" keys in the submit_args dict.
	
	# MRC: This is good to know. We weren't sure if it would be useful to allow
	# custom attributes here, or if we wanted to stick to a pre-defined list.
	"append": {"+DWA_Dependency": "$(DWA_Dependency)"}
}

# Calling submit() should also generate all of the (DAG and SDF) files needed
# in the directory set on the DAG object. Alternatively, if there was a 
# dag.writeFiles() function to explicitly just write those files, but not
# submit, that would be useful as well.
submission = dag.Submit(submit_args)

# Submit to HTCondor the same way it currently works for non-DAGs
schedd = htcondor.Schedd()
with schedd.transaction() as txn:
	cluster_id = submission.queue(txn, 1)
	print("Submit.queue returned cluster_id = " + str(cluster_id))
	
