#!/usr/bin/env python

from setuptools import setup, find_packages
from get_git_version import get_git_version

setup(name='joyce',
      version=get_git_version(),
      description='JSON channels',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/py-joyce/',
      packages=['joyce'],
      package_dir={'joyce': 'src'},
      install_requires = ['docutils>=0.3',
                          'sarah>=0.1.0a2',
                          'mirte>=0.1.0a2',
                          'poster>=0.7.0']
      )
