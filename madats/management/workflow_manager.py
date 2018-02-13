"""
`madats.management.workflow_manager`
====================================

.. currentmodule:: madats.management.workflow_manager

:platform: Unix, Mac
:synopsis: Module that parses a workflow description into a DAG for execution

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import yaml
from madats.core.vds import Task
from madats.core.scheduler import Scheduler
from madats.management import execution_manager
import sys

"""
Parse workflows described in YAML 
"""
def parse_yaml(workflow):
    task_list = []
    input_map = {}
    idx = 0
    try:
        with open(workflow, 'r') as wf:
            tasks = yaml.load(wf)
            for t in tasks:
                info = tasks[t]
                command = ''
                if 'command' in info:
                    command = info['command']
                else:
                    print("'command' missing in task description!")
                    sys.exit()
                task = Task(command)
                if 'name' in info:
                    task.name = info['name']
                else:
                    task.name = 'task' + str(idx)
                if 'inputs' in info:
                    task.inputs = info['inputs']
                if 'outputs' in info:
                    task.outputs = info['outputs']
                if 'params' in info:
                    task.params = info['params']
                if 'scheduler' in info:
                    task.scheduler = Scheduler.type(info['scheduler'])
                if task.scheduler != Scheduler.NONE:
                    task.scheduler_opts = info['queue_config']
                for input in task.inputs:
                    if input not in input_map:
                        input_map[input] = []
                    input_map[input].append(task)
                task_list.append(task)
                idx += 1
        return task_list, input_map
    except yaml.YAMLError as e:
        print(e)


"""
build a DAG from the list of workflow tasks
"""
def build_dag(task_list, input_map):
    dag = {}
    for task in task_list:
        dag[task] = []
        outputs = task.outputs
        for output in outputs:
            if output in input_map:
                successors = input_map[output]
                for succ in successors:
                    dag[task].append(succ)
                    succ.add_predecessor(task)
                    task.add_successor(succ)
        
    #for task in task_list:
    #    preds = [pred.name for pred in task.predecessors]
    #    succs = [succ.name for succ in task.successors]

    return dag
            

"""
"""
def dagify(task_list):
    input_map = {}
    for task in task_list:
        for input in task.inputs:
            if input not in input_map:
                input_map[input] = []
            input_map[input].append(task)
    
    dag = build_dag(task_list, input_map)
    return dag

"""
display a workflow DAG
"""
def display(dag):
    for v in dag:
        successor_list = dag[v]
        id_list = [s.name for s in successor_list]
        successors = " ".join(id_list)
        print("{} : {}".format(v.name, successors))

    print("-------------------------")

    for v in dag:
        id_list = [p.name for p in v.predecessors]
        preds = " ".join(id_list)
        print("{} : {}".format(v.name, preds))

    print("-------------------------")

    for v in dag:
        id_list = [p.name for p in v.successors]
        preds = " ".join(id_list)
        print("{} : {}".format(v.name, preds))




"""
Translate a YAML workflow description into a generic workflow DAG
"""
def parse(workflow, language='yaml'):
    if language == 'yaml':
        task_list, input_map = parse_yaml(workflow)
        dag = build_dag(task_list, input_map)
        return dag
    else:
        print('Invalid workflow description language {}'.format(language))
        sys.exit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: {} <workflow.yaml>'.format(sys.argv[0]))
        sys.exit(-1)

    workflow = sys.argv[1]
    dag = parse(workflow)
    display(dag)
