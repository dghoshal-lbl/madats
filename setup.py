#!/usr/bin/env python

from setuptools import setup, find_packages
import sys
import os
import yaml

python_version = sys.version_info

install_deps = []
with open('requirements.txt') as file_requirements:
    install_deps = file_requirements.read().splitlines()

def madats_config():
    print('configuring MaDaTS')
    if 'MADATS_HOME' in os.environ:
        pass
    else:
        print('Setting MADATS_HOME to {}'.format(os.getcwd()))
        os.environ['MADATS_HOME'] = os.getcwd()
    currdir = os.getcwd()
    workdir = os.path.join(currdir, '_tmp')
    scratch = os.path.join(workdir, 'scratch')
    burst = os.path.join(workdir, 'burst')
    archive = os.path.join(workdir, 'archive')        
        
    if not os.path.exists(scratch):
        os.makedirs(scratch)
    if not os.path.exists(burst):
        os.makedirs(burst)
    if not os.path.exists(archive):
        os.makedirs(archive)

    __setup_storage_config__(scratch, burst, archive)
    with open(os.path.join(currdir, 'MADATS_HOME'), 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('export MADATS_HOME={}\n'.format(os.getenv('MADATS_HOME')))
    print('MaDaTS configuration complete. Use `source {}` to setup MADATS_HOME'.format(os.path.join(currdir, 'MADATS_HOME')))
        

def __setup_storage_config__(scratch, burst, archive):
    storage_config = {'system': 'test'}
    storage_config['test'] = {}
    storage_tiers = {'scratch': [scratch, 'ShortTerm', 700],
                     'burst': [burst, 'None', 1600], 
                     'archive': [archive, 'LongTerm', 1]}
    for k in storage_tiers:
        storage_config['test'][k] = {'mount': storage_tiers[k][0],
                                     'persist': storage_tiers[k][1],
                                     'bandwidth': storage_tiers[k][2]}

    storage_yaml = os.path.expandvars('$MADATS_HOME/config/storage.yaml')
    __write_yaml__(storage_config, storage_yaml)


def __write_yaml__(data, yaml_file):
    with open(yaml_file, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

setup(name='MaDaTS',
      version='1.1.2',
      description='Managing Data on Tiered Storage for Scientific Workflows',
      author='Devarshi Ghoshal',
      author_email='dghoshal@lbl.gov',
      url='http://uda.lbl.gov',
      keywords='',
      packages=find_packages(exclude=['ez_setup', 'tests']),
      include_package_data=True,
      zip_safe=False,
      classifiers=['Development Status :: 1 - Alpha',
                   'Intended Audience :: Science/Research',
                   'Natural Language :: English',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 2.7',
                   'Topic :: Scientific/Engineering',
                   'License :: OSI Approved :: BSD License'
      ],
      install_requires=install_deps,
      entry_points={'console_scripts': ['madats = madats.cli:main']},
)

madats_config()
