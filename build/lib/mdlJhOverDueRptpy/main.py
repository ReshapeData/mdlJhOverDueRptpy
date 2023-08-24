#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .rdsjhysdyq import *


def report_query(server,user,password,charset,database):

    mydate = Sys().date()

    conn = pymssql.connect(server=server, user=user, password=password, charset=charset,
                           database=database, as_dict=True)
    cursor = conn.cursor()

    cursor.execute(
        f"select * from rds_jh_ar_date where  FDateText <= '{mydate}' and FStatus = 0 order by fdatetext desc")

    res = cursor.fetchall()

    sender_main = 'zhangjibin@jaour.com'

    passwd_main = 'UkjxVCBSjRjyvJSg'

    to_receiver_main = ["zhuqiong@jaour.com"]
    CC_receiver_main = ["zhangjibin@jaour.com"]

    for i in res:
        endDate = i['FDateText']

        print(endDate)

        rds_jh(calDate=endDate,server=server,user=user,password=password,charset=charset,database=database)

        title_main = endDate
        content_main = f"""
                        hello everyone:
                            您好,{title_main}业务应收逾期报表更新已完成，请查收！
                        """

        attachment_main = ''

        sendemail(
            sender=sender_main,
            passwd=passwd_main,
            to_receiver=to_receiver_main,
            cc_receiver=CC_receiver_main,
            title=title_main,
            content=content_main
        )

        sql2 = "update a set a.fstatus = 1   from rds_jh_ar_date  a where fdatetext ='" + endDate + "'"

        cursor.execute(sql2)

        conn.commit()


    return True

