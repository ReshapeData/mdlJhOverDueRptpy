#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlJhOverDueRptpy import *
import pytest


@pytest.mark.parametrize('server,user,password,charset,database,output',
                         [("*****", "*****","*****","utf8","******", True)])
def test_report_query(server,user,password,charset,database,output):

    assert report_query(server=server, user=user, password=password,charset=charset,database=database) == output