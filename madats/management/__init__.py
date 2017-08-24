import argparse
from madats.management import workflow_manager, execution_manager
from madats.utils.constants import ExecutionMode
from madats.core import coordinator

def execute(args):
    workflow = args.workflow
    language = args.language
    mode = ExecutionMode.type(args.mode)

    vds = coordinator.map(workflow, language)
    coordinator.plan(vds)
    coordinator.manage(vds, mode)
    coordinator.destroy(vds)

def main():
    parser = argparse.ArgumentParser(description="",
                                     prog="madats",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.set_defaults(func=execute)
    parser.add_argument('-w','--workflow', help='workflow description file', required=True)
    parser.add_argument('-l','--language', help='workflow description language', default='yaml')
    parser.add_argument('-m','--mode', help='execution mode', choices=['dag', 'bin', 'priority', 'dependency'], default='dag')
    
    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
