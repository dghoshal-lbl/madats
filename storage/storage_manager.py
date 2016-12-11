#! /usr/bin/env python

'''
fill in module to manage the storage systems and provide namespaces and mount points
for tiered storage
'''

storage_map = {'scratch': '/scratch/', 'burst': '/dw/'}

def get_storage_info(data_object):
    # not correct, need to fix it with starts with etc.
    for k in storage_map:
        if storage_map[k] in data_object:
            relative_path = data_object.replace(storage_map[k], '')
            #print(data_object, relative_path)
            return k, relative_path
    return None, None
