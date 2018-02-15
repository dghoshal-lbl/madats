"""
`madats.management.workflow_manager`
====================================

.. currentmodule:: madats.management.workflow_manager

:platform: Unix, Mac
:synopsis: Module that parses a workflow description into a DAG for execution

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

## TODO: revisit this later: have make the inputs and outputs as VDOs

import yaml
from madats.core.vds import Task
from madats.core.scheduler import Scheduler
from madats.management import execution_manager
from madats.core.vds import VirtualDataSpace 
import sys

"""
Parse workflows described in YAML 
"""
def parse_yaml(workflow):
    try:
        with open(workflow, 'r') as wf:
            tasks = yaml.load(wf)
            vds = parse_tasks(tasks)
            return vds
    except yaml.YAMLError as e:
        print(e)


def parse_tasks(tasks):
    idx = 0
    vds = VirtualDataSpace()
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
        param_vdo_map = {}
        if 'vin' in info:
            for input in info['vin']:
                vdo = vds.map(input)
                vdo.add_consumer(task)
                param_vdo_map[input] = vdo
        if 'vout' in info:
            for output in info['vout']:
                vdo = vds.map(output)
                vdo.add_producer(task)
                param_vdo_map[output] = vdo
        if 'params' in info:
            for param in info['params']:
                if param in param_vdo_map:
                    task.params.append(param_vdo_map[param])
                else:
                    task.params.append(param)
        if 'scheduler' in info:
            task.scheduler = Scheduler.type(info['scheduler'])
        if task.scheduler != Scheduler.NONE:
            task.scheduler_opts = info['queue_config']
        idx += 1
    return vds


"""
display a workflow DAG
"""
def display(dag):
    print("-------------------------")
    print("{Node: Successor} view")
    print("-------------------------")
    for v in dag:
        successor_list = dag[v]
        id_list = [s.name for s in successor_list]
        successors = " ".join(id_list)
        print("{} : {}".format(v.name, successors))

    print("-------------------------")
    print("{Node: Predecessor} view")
    print("-------------------------")

    for v in dag:
        id_list = [p.name for p in v.predecessors]
        preds = " ".join(id_list)
        print("{} : {}".format(v.name, preds))

    print("-------------------------")
    print("{Node: Successor} view")
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
        vds = parse_yaml(workflow)
        return vds
    elif language == 'DictObj':
        vds = parse_tasks(workflow)
        return vds
    else:
        print('Invalid workflow description language {}'.format(language))
        sys.exit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: {} <workflow.yaml>'.format(sys.argv[0]))
        sys.exit(-1)

    workflow = sys.argv[1]
    vds = parse(workflow)
    display(vds.get_task_dag())
