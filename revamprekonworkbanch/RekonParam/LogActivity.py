#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
from datetime import date, datetime

def cekLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDate) :
    try:
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(Config.ConnLogActivity['user'])+";PWD="+str(Config.ConnLogActivity['password']))
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select [No], DataDate, DataDateString, StartDate, EndDate, TotalData, FlagDone, Note from LogActivity.dbo.LogActivity with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and DataDate='"+str(LogActivityDate)+"' ")
        # print(qryGetTanggal)
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

def getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity) :
    try:
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(Config.ConnLogActivity['user'])+";PWD="+str(Config.ConnLogActivity['password']))
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select [No], DataDate, DataDateString from LogActivity.dbo.LogActivity with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and FlagDone is NULL Order by DataDate")
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

def updateLogActivity(IPServer, ID, FlagDone, TotalData=str('0')) :
    try:
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(Config.ConnLogActivity['user'])+";PWD="+str(Config.ConnLogActivity['password']))
        curLogActivity = conLogActivity.cursor()
        if FlagDone==str('0') :
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET StartDate=GetDate(), Note = 'Running' where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('1') :
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), Note = 'Done', TotalData='"+str(TotalData)+"', FlagDone=1 where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('2') : #Error
            qryUpdateTanggal = ("Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), Note='Error' where [No]='"+str(ID)+"'")
            curLogActivity.execute(qryUpdateTanggal)
            conLogActivity.commit()
            return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        curLogActivity.close()