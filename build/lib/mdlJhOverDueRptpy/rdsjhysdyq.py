import datetime
import time
from _decimal import Decimal
from itertools import chain
import pandas as pd

import pymssql
from pymssql import _mssql
from pymssql import _pymssql
import numpy as np
# from pyrda.dbms.rds import RdClient
from pyrdo.sys import Sys

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from email.mime.application import MIMEApplication


def rds_jh(calDate,server,user,password,charset,database):
    time_start = time.time()
    conn = pymssql.connect(server=server, user=user, password=password, charset=charset,
                           database=database)
    cursor = conn.cursor()
    cursor.callproc("usp_rds_ar", ['%s' % calDate])
    cursor.execute('truncate table rds_detail_ar')
    cursor.execute('truncate table rds_detail_receive')
    cursor.execute('commit')
    sql = "select * from rds_src_ar where fsaledeptname =N'外贸销售部' order by fdate"

    cursor.execute(sql)
    rds_src_ar = cursor.fetchall()

    sql = "select * from rds_src_receive where fsaledeptname= N'外贸销售部' order by fdate"
    cursor.execute(sql)
    rds_src_receive = cursor.fetchall()


    series = 1

    # 应收单正负数核销
    def rule_ar(rds_src_ar):
        for index_a in range(len(rds_src_ar)):

            if int(rds_src_ar[index_a][18]) < 0:
                for index_b in range(len(rds_src_ar)):
                    if rds_src_ar[index_a][2] == rds_src_ar[index_b][2] and rds_src_ar[index_a][5] == \
                            rds_src_ar[index_b][5] and \
                            rds_src_ar[index_a][7] == rds_src_ar[index_b][7] and index_a != index_b and \
                            rds_src_ar[index_b][18] != 0 and \
                            rds_src_ar[index_a][18] == -rds_src_ar[index_b][18]:
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_b] = np.array(rds_src_ar[index_b])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][18] + rds_src_ar[index_a][16]  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][19] + rds_src_ar[index_a][17]  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = 0  # 未回款金额
                        rds_src_ar[index_a][19] = 0  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        rds_src_ar[index_b][16] = rds_src_ar[index_b][16] + rds_src_ar[index_b][18]  # 更新已回款金额
                        rds_src_ar[index_b][17] = rds_src_ar[index_b][17] + rds_src_ar[index_b][19]  # 累计回款金额本位币
                        rds_src_ar[index_b][18] = 0  # 未回款金额
                        rds_src_ar[index_b][19] = 0  # 未回款金额本位币
                        rds_src_ar[index_b][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_b][21] = 'Y'  # 核销标志
                        sql_update1 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_a][16], rds_src_ar[index_a][17], rds_src_ar[index_a][18],
                            rds_src_ar[index_a][19], rds_src_ar[index_a][20], rds_src_ar[index_a][21],
                            rds_src_ar[index_a][9], rds_src_ar[index_a][12])  # 更新源数据表数据
                        sql_update2 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_b][16], rds_src_ar[index_b][17], rds_src_ar[index_b][18],
                            rds_src_ar[index_b][19], rds_src_ar[index_b][20], rds_src_ar[index_b][21],
                            rds_src_ar[index_b][9], rds_src_ar[index_b][12])
                        cursor.execute(sql_update1)
                        cursor.execute(sql_update2)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_ar[index_b] = rds_src_ar[index_b].tolist()
                        print('应收正负相等核销' + str(index_a) + ' ' + str(index_b))
                        break
                    elif rds_src_ar[index_a][2] == rds_src_ar[index_b][2] and rds_src_ar[index_a][5] == \
                            rds_src_ar[index_b][5] and \
                            rds_src_ar[index_a][7] == rds_src_ar[index_b][7] and index_a != index_b and \
                            rds_src_ar[index_b][18] != 0 and \
                            -rds_src_ar[index_a][18] < rds_src_ar[index_b][18]:
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_b] = np.array(rds_src_ar[index_b])
                        tmp = rds_src_ar[index_a][18]
                        tmpf = rds_src_ar[index_a][19]
                        rds_src_ar[index_a][16] = tmp + rds_src_ar[index_a][16]  # 更新已回款金额
                        rds_src_ar[index_a][17] = tmpf + rds_src_ar[index_a][17]  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = 0  # 未回款金额
                        rds_src_ar[index_a][19] = 0  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        rds_src_ar[index_b][16] = rds_src_ar[index_b][16] - tmp  # 更新已回款金额
                        rds_src_ar[index_b][17] = rds_src_ar[index_b][17] - tmpf  # 累计回款金额本位币
                        rds_src_ar[index_b][18] = rds_src_ar[index_b][18] + tmp  # 未回款金额
                        rds_src_ar[index_b][19] = rds_src_ar[index_b][19] + tmpf  # 未回款金额本位币
                        rds_src_ar[index_b][20] = 'N'  # 回款关闭标志
                        rds_src_ar[index_b][21] = 'Y'  # 核销标志
                        sql_update1 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_a][16], rds_src_ar[index_a][17], rds_src_ar[index_a][18],
                            rds_src_ar[index_a][19], rds_src_ar[index_a][20], rds_src_ar[index_a][21],
                            rds_src_ar[index_a][9], rds_src_ar[index_a][12])  # 更新源数据表数据
                        sql_update2 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_b][16], rds_src_ar[index_b][17], rds_src_ar[index_b][18],
                            rds_src_ar[index_b][19], rds_src_ar[index_b][20], rds_src_ar[index_b][21],
                            rds_src_ar[index_b][9], rds_src_ar[index_b][12])
                        cursor.execute(sql_update1)
                        cursor.execute(sql_update2)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_ar[index_b] = rds_src_ar[index_b].tolist()
                        print('应收正大于负核销' + str(index_a) + ' ' + str(index_b))
                        break
        for index_a in range(len(rds_src_ar)):
            if rds_src_ar[index_a][18] < 0:
                for index_b in range(len(rds_src_ar)):
                    if rds_src_ar[index_a][2] == rds_src_ar[index_b][2] and rds_src_ar[index_a][5] == \
                            rds_src_ar[index_b][5] and \
                            rds_src_ar[index_a][7] == rds_src_ar[index_b][7] and index_a != index_b and \
                            rds_src_ar[index_b][18] != 0 and \
                            -rds_src_ar[index_a][18] >= rds_src_ar[index_b][18]:
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_b] = np.array(rds_src_ar[index_b])
                        tmp = rds_src_ar[index_b][18]
                        tmpf = rds_src_ar[index_b][19]
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] - tmp  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] - tmpf  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] + tmp  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] + tmpf  # 未回款金额本位币
                        if rds_src_ar[index_a][18] == 0:
                            rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        else:
                            rds_src_ar[index_a][20] = 'N'
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        rds_src_ar[index_b][16] = rds_src_ar[index_b][16] + tmp  # 更新已回款金额
                        rds_src_ar[index_b][17] = rds_src_ar[index_b][17] + tmpf  # 累计回款金额本位币
                        rds_src_ar[index_b][18] = 0  # 未回款金额
                        rds_src_ar[index_b][19] = 0  # 未回款金额本位币
                        rds_src_ar[index_b][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_b][21] = 'Y'  # 核销标志
                        sql_update1 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_a][16], rds_src_ar[index_a][17], rds_src_ar[index_a][18],
                            rds_src_ar[index_a][19], rds_src_ar[index_a][20], rds_src_ar[index_a][21],
                            rds_src_ar[index_a][9], rds_src_ar[index_a][12])  # 更新源数据表数据
                        sql_update2 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_b][16], rds_src_ar[index_b][17], rds_src_ar[index_b][18],
                            rds_src_ar[index_b][19], rds_src_ar[index_b][20], rds_src_ar[index_b][21],
                            rds_src_ar[index_b][9], rds_src_ar[index_b][12])
                        cursor.execute(sql_update1)
                        cursor.execute(sql_update2)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_ar[index_b] = rds_src_ar[index_b].tolist()
                        print('应收正小于负核销' + str(index_a) + ' ' + str(index_b))
                    elif rds_src_ar[index_a][2] == rds_src_ar[index_b][2] and rds_src_ar[index_a][5] == \
                            rds_src_ar[index_b][5] and \
                            rds_src_ar[index_a][7] == rds_src_ar[index_b][7] and index_a != index_b and \
                            rds_src_ar[index_b][18] != 0 and \
                            -rds_src_ar[index_a][18] < rds_src_ar[index_b][18]:
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_b] = np.array(rds_src_ar[index_b])
                        tmp = rds_src_ar[index_a][18]
                        tmpf = rds_src_ar[index_a][19]
                        rds_src_ar[index_a][16] = tmp + rds_src_ar[index_a][16]  # 更新已回款金额
                        rds_src_ar[index_a][17] = tmpf + rds_src_ar[index_a][17]  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = 0  # 未回款金额
                        rds_src_ar[index_a][19] = 0  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        rds_src_ar[index_b][16] = rds_src_ar[index_b][16] - tmp  # 更新已回款金额
                        rds_src_ar[index_b][17] = rds_src_ar[index_b][17] - tmpf  # 累计回款金额本位币
                        rds_src_ar[index_b][18] = rds_src_ar[index_b][18] + tmp  # 未回款金额
                        rds_src_ar[index_b][19] = rds_src_ar[index_b][19] + tmpf  # 未回款金额本位币
                        rds_src_ar[index_b][20] = 'N'  # 回款关闭标志
                        rds_src_ar[index_b][21] = 'Y'  # 核销标志
                        sql_update1 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_a][16], rds_src_ar[index_a][17], rds_src_ar[index_a][18],
                            rds_src_ar[index_a][19], rds_src_ar[index_a][20], rds_src_ar[index_a][21],
                            rds_src_ar[index_a][9], rds_src_ar[index_a][12])  # 更新源数据表数据
                        sql_update2 = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            rds_src_ar[index_b][16], rds_src_ar[index_b][17], rds_src_ar[index_b][18],
                            rds_src_ar[index_b][19], rds_src_ar[index_b][20], rds_src_ar[index_b][21],
                            rds_src_ar[index_b][9], rds_src_ar[index_b][12])
                        cursor.execute(sql_update1)
                        cursor.execute(sql_update2)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_ar[index_b] = rds_src_ar[index_b].tolist()
                        print('应收正大于负核销' + str(index_a) + ' ' + str(index_b))
                        break

    # 规则A对有根据客户+销售订单进行核销
    def rule_a(rds_src_ar, rds_src_receive, series):
        for index_a in range(len(rds_src_ar)):  # 首先对每个应收单进行循环，去匹配对应的收款单
            if len(rds_src_ar[index_a][12].replace(' ', '')) == 0:  # 如果当前应收单的销售订单为空
                continue  # 则进入下一条应收单
            else:  # 如果销售订单不为空，则遍历收款单进行核销
                for index_c in range(len(rds_src_receive)):  # 遍历收款单
                    if rds_src_ar[index_a][12] == rds_src_receive[index_c][11] and rds_src_receive[index_c][14] != 0 and \
                            rds_src_ar[index_a][18] != 0 and \
                            rds_src_ar[index_a][18] == rds_src_receive[index_c][
                        14]:  # 如果销售订单号相同且收款单未被核销且金额相同#销售订单号相同，核销应收不为0，回款金额不为0
                        # 应收单数据处理
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] + rds_src_receive[index_c][9]  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] + rds_src_receive[index_c][10]  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] - rds_src_receive[index_c][9]  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] - rds_src_receive[index_c][10]  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        tmp_rsa = rds_src_ar[index_a].tolist()
                        tmp_rsa.append(rds_src_receive[index_c][9])  # 本次核销金额
                        tmp_rsa.append(series)  # 核销序号
                        tmp_rsa.append('A')  # 核销规则
                        # 收款单数据处理
                        rds_src_receive[index_c] = np.array(rds_src_receive[index_c])
                        rds_src_receive[index_c][12] = rds_src_receive[index_c][12] + rds_src_receive[index_c][
                            9]  # 核销应收金额
                        rds_src_receive[index_c][13] = rds_src_receive[index_c][13] + rds_src_receive[index_c][
                            10]  # 核销应收金额本位币
                        rds_src_receive[index_c][14] = rds_src_receive[index_c][14] - rds_src_receive[index_c][
                            9]  # 未核销应收金额
                        rds_src_receive[index_c][15] = rds_src_receive[index_c][15] - rds_src_receive[index_c][
                            10]  # 未核销应收金额本位币
                        rds_src_receive[index_c][16] = 'Y'  # 核销标志
                        tmp_rsc = rds_src_receive[index_c].tolist()
                        tmp_rsc.append(rds_src_receive[index_c][9])  # 本次核销金额
                        tmp_rsc.append(series)  # 核销序号
                        tmp_rsc.append('A')  # 核销规则
                        print('核销金额相等A' + str(series))

                        series = 1 + series
                        sql_rda = 'INSERT INTO rds_detail_ar VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsa), )  # 数据写入应收中间表
                        sql_rdc = 'INSERT INTO rds_detail_receive VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsc), )  # 数据写入收款中间表
                        sql_rsa = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsa[16], tmp_rsa[17], tmp_rsa[18], tmp_rsa[19], tmp_rsa[20], tmp_rsa[21], tmp_rsa[9],
                            tmp_rsa[12])  # 更新源数据表数据
                        sql_rsc = "update rds_src_receive set FClearArAmt=%s,FClearArAmt_LC=%s,FUnClearArAmt=%s,FUnClearArAmt_LC=%s,FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsc[12], tmp_rsc[13], tmp_rsc[14], tmp_rsc[15], tmp_rsc[16], tmp_rsc[8],
                            tmp_rsc[11])  # 更新源数据表数据

                        cursor.execute(sql_rda)
                        cursor.execute(sql_rdc)
                        cursor.execute(sql_rsa)
                        cursor.execute(sql_rsc)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_receive[index_c] = rds_src_receive[index_c].tolist()

        for index_a in range(len(rds_src_ar)):  # 首先对每个应收单进行循环，去匹配对应的收款单
            if len(rds_src_ar[index_a][12].replace(' ', '')) == 0:  # 如果当前应收单的销售订单为空
                continue  # 则进入下一条应收单
            else:  # 如果销售订单不为空，则遍历收款单进行核销
                for index_c in range(len(rds_src_receive)):  # 遍历收款单
                    if rds_src_ar[index_a][12] == rds_src_receive[index_c][11] and rds_src_receive[index_c][14] != 0 and \
                            rds_src_ar[index_a][18] != 0 and \
                            rds_src_ar[index_a][18] < rds_src_receive[index_c][
                        14]:  # 如果销售订单号相同且收款单未被核销且应收单未收款金额<收款单未核销金额#销售订单号相同，核销应收不为0，回款金额不为0
                        # 应收单数据处理
                        tmp_hx = rds_src_ar[index_a][18]
                        tmp_hx_b = rds_src_ar[index_a][19]
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] + tmp_hx  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] + tmp_hx_b  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] - tmp_hx  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] - tmp_hx_b  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        tmp_rsa = rds_src_ar[index_a].tolist()
                        tmp_rsa.append(tmp_hx)  # 本次核销金额
                        tmp_rsa.append(series)  # 核销序号
                        tmp_rsa.append('A')  # 核销规则

                        # 收款单数据处理
                        rds_src_receive[index_c] = np.array(rds_src_receive[index_c])
                        rds_src_receive[index_c][12] = rds_src_receive[index_c][12] + tmp_hx  # 核销应收金额
                        rds_src_receive[index_c][13] = rds_src_receive[index_c][13] + tmp_hx_b  # 核销应收金额本位币
                        rds_src_receive[index_c][14] = rds_src_receive[index_c][14] - tmp_hx  # 未核销应收金额
                        rds_src_receive[index_c][15] = rds_src_receive[index_c][15] - tmp_hx_b  # 未核销应收金额本位币
                        rds_src_receive[index_c][16] = 'N'  # 核销标志
                        tmp_rsc = rds_src_receive[index_c].tolist()
                        tmp_rsc.append(tmp_hx)  # 本次核销金额
                        tmp_rsc.append(series)  # 核销序号
                        tmp_rsc.append('A')  # 核销规则
                        print('核销应收小于收款A' + str(series))
                        series = 1 + series

                        sql_rda = 'INSERT INTO rds_detail_ar VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsa), )  # 数据写入应收中间表
                        sql_rdc = 'INSERT INTO rds_detail_receive VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsc), )  # 数据写入收款中间表
                        sql_rsa = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsa[16], tmp_rsa[17], tmp_rsa[18], tmp_rsa[19], tmp_rsa[20], tmp_rsa[21], tmp_rsa[9],
                            tmp_rsa[12])  # 更新源数据表数据
                        sql_rsc = "update rds_src_receive set FClearArAmt=%s,FClearArAmt_LC=%s,FUnClearArAmt=%s,FUnClearArAmt_LC=%s,FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsc[12], tmp_rsc[13], tmp_rsc[14], tmp_rsc[15], tmp_rsc[16], tmp_rsc[8],
                            tmp_rsc[11])  # 更新源数据表数据
                        cursor.execute(sql_rda)
                        cursor.execute(sql_rdc)
                        cursor.execute(sql_rsa)
                        cursor.execute(sql_rsc)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_receive[index_c] = rds_src_receive[index_c].tolist()

                    elif rds_src_ar[index_a][12] == rds_src_receive[index_c][11] and rds_src_receive[index_c][
                        14] != 0 and \
                            rds_src_ar[index_a][18] != 0 and \
                            rds_src_ar[index_a][18] >= rds_src_receive[index_c][
                        14]:  # 如果销售订单号相同且收款单未被核销且应收单未收款金额>=收款单未核销金额#销售订单号相同，核销应收不为0，回款金额不为0
                        # 应收单数据处理
                        tmp_hx = rds_src_receive[index_c][14]
                        tmp_hx_b = rds_src_receive[index_c][15]
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] + tmp_hx  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] + tmp_hx_b  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] - tmp_hx  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] - tmp_hx_b  # 未回款金额本位币
                        if rds_src_ar[index_a][18] == 0:
                            rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        else:
                            rds_src_ar[index_a][20] = 'N'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        tmp_rsa = rds_src_ar[index_a].tolist()
                        tmp_rsa.append(tmp_hx)  # 本次核销金额
                        tmp_rsa.append(series)  # 核销序号
                        tmp_rsa.append('A')  # 核销规则

                        # 收款单数据处理
                        rds_src_receive[index_c] = np.array(rds_src_receive[index_c])
                        rds_src_receive[index_c][12] = rds_src_receive[index_c][12] + tmp_hx  # 核销应收金额
                        rds_src_receive[index_c][13] = rds_src_receive[index_c][13] + tmp_hx_b  # 核销应收金额本位币
                        rds_src_receive[index_c][14] = rds_src_receive[index_c][14] - tmp_hx  # 未核销应收金额
                        rds_src_receive[index_c][15] = rds_src_receive[index_c][15] - tmp_hx_b  # 未核销应收金额本位币
                        rds_src_receive[index_c][16] = 'Y'  # 核销标志
                        tmp_rsc = rds_src_receive[index_c].tolist()
                        tmp_rsc.append(tmp_hx)  # 本次核销金额
                        tmp_rsc.append(series)  # 核销序号
                        tmp_rsc.append('A')  # 核销规则
                        print('核销应收大于等于收款A' + str(series))
                        series = 1 + series

                        sql_rda = 'INSERT INTO rds_detail_ar VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsa), )  # 数据写入应收中间表
                        sql_rdc = 'INSERT INTO rds_detail_receive VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsc), )  # 数据写入收款中间表
                        sql_rsa = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsa[16], tmp_rsa[17], tmp_rsa[18], tmp_rsa[19], tmp_rsa[20], tmp_rsa[21], tmp_rsa[9],
                            tmp_rsa[12])  # 更新源数据表数据
                        sql_rsc = "update rds_src_receive set FClearArAmt=%s,FClearArAmt_LC=%s,FUnClearArAmt=%s,FUnClearArAmt_LC=%s,FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsc[12], tmp_rsc[13], tmp_rsc[14], tmp_rsc[15], tmp_rsc[16], tmp_rsc[8],
                            tmp_rsc[11])  # 更新源数据表数据

                        cursor.execute(sql_rda)
                        cursor.execute(sql_rdc)
                        cursor.execute(sql_rsa)
                        cursor.execute(sql_rsc)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_receive[index_c] = rds_src_receive[index_c].tolist()
        return series

    # 规则B对有根据客户进行核销
    def rule_b(rds_src_ar, rds_src_receive, series):
        while len([x for x in rds_src_receive if x[14] != 0]) != 0:  # 当收款单未核销金额不为0时，一直循环核销直至核销成功
            tmp_hx = 0  # 重置本次核销金额
            for index_a in range(len(rds_src_ar)):  # 首先对每个应收单进行循环，去匹配对应的收款单
                for index_c in range(len(rds_src_receive)):  # 遍历收款单
                    if rds_src_ar[index_a][5] == rds_src_receive[index_c][4] and rds_src_ar[index_a][7] == \
                            rds_src_receive[index_c][6] and rds_src_ar[index_a][2] == rds_src_receive[index_c][1] and \
                            rds_src_receive[index_c][14] != 0 and \
                            rds_src_ar[index_a][18] < rds_src_receive[index_c][14] and rds_src_ar[index_a][
                        18] != 0:  # 如应收单未收款金额<收款单未核销金额#客户代码相同，币别相同，销售组织相同，核销应收不为0，回款金额不为0
                        # 应收单数据处理
                        tmp_hx = rds_src_ar[index_a][18]
                        tmp_hx_b = rds_src_ar[index_a][19]
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] + tmp_hx  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] + tmp_hx_b  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] - tmp_hx  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] - tmp_hx_b  # 未回款金额本位币
                        rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        tmp_rsa = rds_src_ar[index_a].tolist()
                        tmp_rsa.append(tmp_hx)  # 本次核销金额
                        tmp_rsa.append(series)  # 核销序号
                        tmp_rsa.append('B')  # 核销规则

                        # 收款单数据处理
                        rds_src_receive[index_c] = np.array(rds_src_receive[index_c])
                        rds_src_receive[index_c][12] = rds_src_receive[index_c][12] + tmp_hx  # 核销应收金额
                        rds_src_receive[index_c][13] = rds_src_receive[index_c][13] + tmp_hx_b  # 核销应收金额本位币
                        rds_src_receive[index_c][14] = rds_src_receive[index_c][14] - tmp_hx  # 未核销应收金额
                        rds_src_receive[index_c][15] = rds_src_receive[index_c][15] - tmp_hx_b  # 未核销应收金额本位币
                        rds_src_receive[index_c][16] = 'N'  # 核销标志
                        tmp_rsc = rds_src_receive[index_c].tolist()
                        tmp_rsc.append(tmp_hx)  # 本次核销金额
                        tmp_rsc.append(series)  # 核销序号
                        tmp_rsc.append('B')  # 核销规则
                        print('核销应收小于收款B' + str(series))
                        series = 1 + series

                        sql_rda = 'INSERT INTO rds_detail_ar VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsa), )  # 数据写入应收中间表
                        sql_rdc = 'INSERT INTO rds_detail_receive VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsc), )  # 数据写入收款中间表
                        sql_rsa = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsa[16], tmp_rsa[17], tmp_rsa[18], tmp_rsa[19], tmp_rsa[20], tmp_rsa[21], tmp_rsa[9],
                            tmp_rsa[12])  # 更新源数据表数据
                        sql_rsc = "update rds_src_receive set FClearArAmt=%s,FClearArAmt_LC=%s,FUnClearArAmt=%s,FUnClearArAmt_LC=%s,FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsc[12], tmp_rsc[13], tmp_rsc[14], tmp_rsc[15], tmp_rsc[16], tmp_rsc[8],
                            tmp_rsc[11])  # 更新源数据表数据
                        cursor.execute(sql_rda)
                        cursor.execute(sql_rdc)
                        cursor.execute(sql_rsa)
                        cursor.execute(sql_rsc)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_receive[index_c] = rds_src_receive[index_c].tolist()
                    elif rds_src_ar[index_a][5] == rds_src_receive[index_c][4] and rds_src_ar[index_a][7] == \
                            rds_src_receive[index_c][6] and rds_src_ar[index_a][2] == rds_src_receive[index_c][1] and \
                            rds_src_receive[index_c][14] != 0 and \
                            rds_src_ar[index_a][18] >= rds_src_receive[index_c][14] and rds_src_ar[index_a][
                        18] != 0:  # 应收单未收款金额>=收款单未核销金额#客户代码相同，币别相同，销售组织相同，核销应收不为0，回款金额不为0
                        # 应收单数据处理
                        tmp_hx = rds_src_receive[index_c][14]
                        tmp_hx_b = rds_src_receive[index_c][15]
                        rds_src_ar[index_a] = np.array(rds_src_ar[index_a])
                        rds_src_ar[index_a][16] = rds_src_ar[index_a][16] + tmp_hx  # 更新已回款金额
                        rds_src_ar[index_a][17] = rds_src_ar[index_a][17] + tmp_hx_b  # 累计回款金额本位币
                        rds_src_ar[index_a][18] = rds_src_ar[index_a][18] - tmp_hx  # 未回款金额
                        rds_src_ar[index_a][19] = rds_src_ar[index_a][19] - tmp_hx_b  # 未回款金额本位币
                        if rds_src_ar[index_a][18] == 0:
                            rds_src_ar[index_a][20] = 'Y'  # 回款关闭标志
                        else:
                            rds_src_ar[index_a][20] = 'N'  # 回款关闭标志
                        rds_src_ar[index_a][21] = 'Y'  # 核销标志
                        tmp_rsa = rds_src_ar[index_a].tolist()
                        tmp_rsa.append(tmp_hx)  # 本次核销金额
                        tmp_rsa.append(series)  # 核销序号
                        tmp_rsa.append('B')  # 核销规则

                        # 收款单数据处理
                        rds_src_receive[index_c] = np.array(rds_src_receive[index_c])
                        rds_src_receive[index_c][12] = rds_src_receive[index_c][12] + tmp_hx  # 核销应收金额
                        rds_src_receive[index_c][13] = rds_src_receive[index_c][13] + tmp_hx_b  # 核销应收金额本位币
                        rds_src_receive[index_c][14] = rds_src_receive[index_c][14] - tmp_hx  # 未核销应收金额
                        rds_src_receive[index_c][15] = rds_src_receive[index_c][15] - tmp_hx_b  # 未核销应收金额本位币
                        rds_src_receive[index_c][16] = 'Y'  # 核销标志
                        tmp_rsc = rds_src_receive[index_c].tolist()
                        tmp_rsc.append(tmp_hx)  # 本次核销金额
                        tmp_rsc.append(series)  # 核销序号
                        tmp_rsc.append('B')  # 核销规则
                        print('核销应收大于等于收款B' + str(series))
                        series = 1 + series

                        sql_rda = 'INSERT INTO rds_detail_ar VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsa), )  # 数据写入应收中间表
                        sql_rdc = 'INSERT INTO rds_detail_receive VALUES (%s)' % ','.join(
                            (repr(str(x).replace("'", "1")) for x in tmp_rsc), )  # 数据写入收款中间表
                        sql_rsa = "update rds_src_ar set FCumsumReceiveAmt = %s,FCumsumReceiveAmt_LC= %s,FUnReceiveAmt= %s,FUnReceiveAmt_LC=%s,FReceiveFlag='%s',FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsa[16], tmp_rsa[17], tmp_rsa[18], tmp_rsa[19], tmp_rsa[20], tmp_rsa[21], tmp_rsa[9],
                            tmp_rsa[12])  # 更新源数据表数据
                        sql_rsc = "update rds_src_receive set FClearArAmt=%s,FClearArAmt_LC=%s,FUnClearArAmt=%s,FUnClearArAmt_LC=%s,FClearingFlag='%s' where fbillno='%s' and FSoNumber='%s'" % (
                            tmp_rsc[12], tmp_rsc[13], tmp_rsc[14], tmp_rsc[15], tmp_rsc[16], tmp_rsc[8],
                            tmp_rsc[11])  # 更新源数据表数据
                        cursor.execute(sql_rda)
                        cursor.execute(sql_rdc)

                        cursor.execute(sql_rsa)
                        cursor.execute(sql_rsc)
                        rds_src_ar[index_a] = rds_src_ar[index_a].tolist()
                        rds_src_receive[index_c] = rds_src_receive[index_c].tolist()
            if tmp_hx == 0:  # 没循环出本次核销金额时退出循环
                print('无可核销收款单')
                break
        return series

    rule_ar(rds_src_ar)
    series = rule_a(rds_src_ar, rds_src_receive, series)
    series = rule_b(rds_src_ar, rds_src_receive, series)

    cursor.execute("select * from rds_src_ar where fsaledeptname=N'内贸销售部' order by fdate")

    rds_src_ar = cursor.fetchall()

    cursor.execute("select * from rds_src_receive where fsaledeptname=N'内贸销售部' order by fdate")
    rds_src_receive = cursor.fetchall()

    rule_ar(rds_src_ar)
    series = rule_b(rds_src_ar, rds_src_receive, series)

    cursor.execute("select * from rds_src_ar where fsaledeptname=N'采购部' order by fdate")
    rds_src_ar = cursor.fetchall()

    cursor.execute("select * from rds_src_receive where fsaledeptname=N'采购部' order by fdate")
    rds_src_receive = cursor.fetchall()

    rule_ar(rds_src_ar)
    series = rule_b(rds_src_ar, rds_src_receive, series)

    sql = 'delete t_rds_ar where 截止日期 = %s' % repr(calDate)
    cursor.execute(sql)
    sql = 'insert into t_rds_ar  select * from v_rds_ar'
    cursor.execute(sql)
    cursor.close()
    conn.close()
    time_end = time.time()
    print('totally cost', (time_end - time_start) / 60)
    
    
def sendemail(sender, passwd,to_receiver: list, cc_receiver: list, title='嘉好业务应收逾期报表通知', content='报表更新已完成', attachment=''):
    """
    sender，发送者
    passwd，发送人邮箱授权码。这个授权码,是在网易邮箱里设置里设置的三方授权码。
    to_receiver，接受人，可以传列表，给多个人发
    cc_receiver，抄送人，可以传列表，给多个人发
    title，邮箱标题
    content，邮件内容
    attachment，附件。传一个地址
    """
    # 1、设置发送者
    msg = MIMEMultipart()  # MIMEMultipart类可以放任何内容
    my_sender = sender
    my_pass = passwd
    # 接受者
    my_to_receiver = to_receiver
    my_cc_receiver = cc_receiver
    receiver = my_to_receiver + my_cc_receiver
    msg['From'] = formataddr(('发送者', my_sender))
    msg['To'] = ",".join(my_to_receiver)
    msg['Cc'] = ",".join(my_cc_receiver)

    # 2、设置邮件标题
    msg['Subject'] = title
    # 3、邮件内容
    my_content = content  # 邮件内容
    msg.attach(MIMEText(my_content, 'plain', 'utf-8'))  # 把内容加进去
    # 4、添加附件
    fujian = attachment  # 定义附件
    if fujian == '':  # 如果没传附件地址，就直接略过
        pass
    else:
        my_att = MIMEApplication(open(fujian, 'rb').read())  # 用二进制读附件
        my_att.add_header('Content-Disposition', 'attachment', filename=('gbk', '', 'phone_section_result.xls'))
        msg.attach(my_att)  # 添加附件
    # 5、发送邮件
    try:
        server = smtplib.SMTP_SSL("smtp.qiye.163.com",465)
        server.login(my_sender, my_pass)
        server.sendmail(my_sender, receiver, msg.as_string())
        print("邮件发送成功")
        server.quit()

    except Exception as n:
        print("Error: 无法发送邮件")
        print(n)



