#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
from datetime import date, datetime
from Etc import passDecrypt

def cekLogActivity(IPServer, DBUser, DBPass, ApplicationIntent, InitialCatalog, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDate) :
    try:
        ApplicationIntentConnection = ''
        InitialCatalogConnection = ''
        if(len(ApplicationIntent)>0) :
            ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
        if(len(InitialCatalog)>0) :
            InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
        conLogActivity = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";DATABASE=LogActivity;UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select [No], DataDate, DataDateString, StartDate, EndDate, TotalData, FlagDone, Note from LogActivity.dbo.LogActivity with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and DataDate='"+str(LogActivityDate)+"' ")
        curLogActivity.execute(qryGetTanggal)
        Tanggals = curLogActivity.fetchall()

        if len(Tanggals) > 0 :
            return {'rc' : True, 'result' : Tanggals}
        else :
            return {'rc' : False}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        curLogActivity.close()

def getLogActivity(IPServer, DBPort, DBUser, DBPass, ApplicationIntent, InitialCatalog,  LogActivityModul, LogActivitySubModul, LogActivityEntity) :
    try:
        ApplicationIntentConnection = ''
        InitialCatalogConnection = ''
        if(len(ApplicationIntent)>0) :
            ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
        if(len(InitialCatalog)>0) :
            InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
        conLogActivity = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE=LogActivity;UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select [No], DataDate, DataDateString from dbo.LogActivity with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and FlagDone is NULL Order by DataDate")
        #print(qryGetTanggal)
        curLogActivity.execute(qryGetTanggal)
        Tanggals = curLogActivity.fetchall()
        if len(Tanggals) > 0 :
            return {'rc' : True, 'result' : Tanggals}
        else :
            return {'rc' : False,  'result' : 'sini'}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'result' : 'exceptionn'}
    finally :
        curLogActivity.close()

def updateLogActivity(IPServer, DBUser, DBPass, ApplicationIntent, InitialCatalog, ID, FlagDone, TotalData=str('0')) :
    try:
        ApplicationIntentConnection = ''
        InitialCatalogConnection = ''
        if(len(ApplicationIntent)>0) :
            ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
        if(len(InitialCatalog)>0) :
            InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
        conLogActivity = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";DATABASE=LogActivity;UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
        curLogActivity = conLogActivity.cursor()
        if FlagDone==str('0') :
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET StartDate=GetDate() where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('1') :
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), TotalData='"+str(TotalData)+"', FlagDone=1 where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('2') : #Error
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), Note='' where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        curLogActivity.close()