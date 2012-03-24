#!/usr/bin/env python

from setuptools import setup, find_packages
from get_git_version import get_git_version
import os, os.path

def find_package_data():
    base = os.path.join(os.path.dirname(__file__), 'src')
    s, r = ['.'], []
    while s:
        p = s.pop()
        for c in os.listdir(os.path.join(base, p)):
            if os.path.isdir(os.path.join(base, p, c)):
                s.append(os.path.join(p, c))
            elif c.endswith('.mirte'):
                r.append(os.path.join(p, c))
    return r

setup(name='joyce',
      version=get_git_version(),
      description='JSON channels',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/py-joyce/',
      packages=['joyce'],
      package_dir={'joyce': 'src'},
      package_data={'joyce': find_package_data()},
      install_requires = ['docutils>=0.3',
                          'sarah>=0.1.2',
                          'mirte>=0.1.3',
                          'poster>=0.7.0']
      )

# vim: et:sta:bs=2:sw=4:
