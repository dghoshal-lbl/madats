import argparse
from madats.management import workflow_manager, execution_manager

def execute(args):
    workflow = args.workflow
    language = args.language
    dag = workflow_manager.parse(workflow, language)
    execution_manager.execute(dag)

def main():
    parser = argparse.ArgumentParser(description="",
                                     prog="mts-wfm",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.set_defaults(func=execute)
    parser.add_argument('-w','--workflow', help='workflow description file', required=True)
    parser.add_argument('-l','--language', help='workflow description language', default='yaml')
    
    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
