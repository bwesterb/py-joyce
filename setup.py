#!/usr/bin/env python

from setuptools import setup, find_packages, findall
from get_git_version import get_git_version

setup(name='joyce',
      version=get_git_version(),
      description='JSON channels',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/py-joyce/',
      packages=['joyce'],
      package_dir={'joyce': 'src'},
      package_data={
          'joyce': [f for f in findall('joyce') if f.endswith('.mirte')] },
      install_requires = ['docutils>=0.3',
                          'sarah>=0.1.1',
                          'mirte>=0.1.1',
                          'poster>=0.7.0']
      )
