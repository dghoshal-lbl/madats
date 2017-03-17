#!/usr/bin/env python

from setuptools import setup, find_packages
import sys

python_version = sys.version_info

install_deps = []
with open('requirements.txt') as file_requirements:
    install_deps = file_requirements.read().splitlines()

setup(name='MaDaTS',
      version='0.1.1',
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
      #entry_points={'console_scripts': ['madats = madats:main']},
      data_files=[('config',['config/config.ini'])]      
)
