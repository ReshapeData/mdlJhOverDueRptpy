#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    reshapedata LLC
"""
import platform
from setuptools import setup
from setuptools import find_packages

setup(
    name  ='mdlJhOverDueRptpy',
    version = '1.0.0',
    install_requires=[
        'requests',
    ],
    packages=find_packages(),
    license = 'Apache License',
    author = 'zhangzhi',
    author_email = '1642699718@qq.com',
    url = 'http://www.reshapedata.com',
    description = 'reshape data type in py language ',
    keywords = ['reshapedata', 'rdt','pyrdt'],
    python_requires='>=3.6',
)
