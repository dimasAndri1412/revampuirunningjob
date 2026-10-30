#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
import pandas as pd
from datetime import timedelta, date, datetime
from Etc import sanitize, passDecrypt
import re


def getServerConnection(IdServer,Keterangan) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>> Get : Parameter Server "+str(Keterangan)+" untuk IdServer "+ str(IdServer))

        conDataFlowParam = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(Config.ConnDataFlowParam['host'])+";DATABASE="+str(Config.ConnDataFlowParam['database'])+";UID="+str(Config.ConnDataFlowParam['user'])+";PWD="+str(Config.ConnDataFlowParam['password']))
        curDataFlowParam = conDataFlowParam.cursor()

        qryGetServerConnection = ("""select 
								IPServer, DBUser, DBPass, DBType, DBPort, IsListenerHA,ApplicationIntent,InitialCatalog 
							from ParamServer with(nolock) 
							where IdServer='{IdServer}'""")
        curDataFlowParam.execute(qryGetServerConnection.format(IdServer=str(sanitize(IdServer))))
        ConServer = curDataFlowParam.fetchone()
        if len(ConServer) > 0 :
            return {'rc' : True, 'result' : ConServer}
        else :
            return {'rc' : False, 'msg' : str('configurasi koneksi tidak ditemukan')}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}
    finally :
        curDataFlowParam.close()

def createLog(IDFitur, Note) :
    try:
        conDataFlowParam = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(Config.ConnDataFlowParam['host'])+";DATABASE="+str(Config.ConnDataFlowParam['database'])+";UID="+str(Config.ConnDataFlowParam['user'])+";PWD="+str(Config.ConnDataFlowParam['password']))
        conDataFlowParam.autocommit = True
        curDataFlowParam = conDataFlowParam.cursor()
        qryGetConFitur = ("Insert into LogExecute (DateExec, IDFitur, Note) values (GetDate(), ?, ?)")
        curDataFlowParam.execute(qryGetConFitur, (IDFitur,Note))
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        curDataFlowParam.close()

def replaceDateParam(DataDate, DateAdd, stringtoreplace):
    formatTgl = re.sub('[\{\}]', '', stringtoreplace)
    toDataDate = datetime.strptime(DataDate, "%Y%m%d")
    Tanggal = toDataDate + timedelta(days=DateAdd)
    return Tanggal.strftime(formatTgl)

def replaceDatetimeParam(DataDate, DateAdd, stringtoreplace):
    formatTgl = re.sub('[\{\}]', '', stringtoreplace)
    print(formatTgl)
    toDataDate = datetime.strptime(DataDate, "%Y%m%d%H%M%S")
    # print(toDataDate)
    Tanggal = toDataDate + timedelta(days=DateAdd)
    return Tanggal.strftime(formatTgl)
