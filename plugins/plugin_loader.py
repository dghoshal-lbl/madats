"""
Plugin naming convention: 
  modulename: <type>.<name>_<type>.py
  classname: <Name><Type>Manager
"""

from utils.config import Config

'''
load data management plugin
'''
def load_data_plugin():
    type = 'data'
    return __plugin__(type)


'''
load execution management plugin
'''
def load_execution_plugin():
    type = 'execution'
    return __plugin__(type)

'''
load copy manegement plugin
'''
def load_copy_plugin():
    type = 'copy'
    return __plugin__(type)

'''
load storage management plugin
'''
def load_storage_plugin():
    type = 'storage'
    return __plugin__(type)

'''
load workflow management plugin
'''
def load_workflow_plugin():
    type = 'workflow'
    return __plugin__(type)

'''
get plugin type from the config file and load it
'''
def __plugin__(type):
    config = Config('config/config.ini')
    name = config.get('plugins', type)
    modulename = type + '.' + name + '_' + type
    classname = name.title() + type.title() + 'Manager'
    plugin = __load_plugin__(modulename, classname, type)
    return plugin

'''
dynamically loads the desired plugin
'''
def __load_plugin__(modulename, classname, category):
    try:
        module = __import__(modulename, globals(), locals(), [classname])
        class_ = getattr(module, classname)

        print('Using {} plugin: {}'.format(category, modulename))
        class_obj = class_()
        return class_obj
    except Exception as e:
        print("No {} plugin : {}, Error: {}".format(category, modulename, e.message))
        raise e
