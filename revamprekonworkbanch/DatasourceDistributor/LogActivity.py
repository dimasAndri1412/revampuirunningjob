#!/usr/bin/python
import Config
import ConnectDB
import sys
import pyodbc
import os
import traceback
from datetime import date, datetime
from Etc import sanitize

def cekLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDate, DBUser=str(''), DBPass=str('')) :
    try:
        if DBUser=='' :
            DBUser=Config.ConnLogActivity['user']
        if DBPass=='' :
            DBPass=Config.ConnLogActivity['password']
        conLogActivity = ConnectDB.connect_LogActivity(IPServer, DBUser, DBPass)
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("""select [No], DataDate, DataDateString, StartDate, EndDate, TotalData, FlagDone, Note from LogActivity.dbo.LogActivity with(nolock) where Modul='{LogActivityModul}' and SubModul='{LogActivitySubModul}' and Entity='{LogActivityEntity}' and DataDate='{LogActivityDate}' """)
        #print(qryGetTanggal)
        #print(LogActivityModul)
        #print(LogActivitySubModul)
        #print(LogActivityEntity)
        #print(LogActivityDate)
        curLogActivity.execute(qryGetTanggal.format(LogActivityModul=sanitize(LogActivityModul), LogActivitySubModul=sanitize(LogActivitySubModul), LogActivityEntity=sanitize(LogActivityEntity), LogActivityDate=sanitize(LogActivityDate)))
        Tanggals = curLogActivity.fetchall()
        if len(Tanggals) > 0 :
            return {'rc' : True, 'result' : Tanggals}
        else :
            return {'rc' : False, 'msg' : 'LogActivity tidak ditemukan'}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}
    finally :
        curLogActivity.close()

def getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, DBUser=str(''), DBPass=str('')) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil Tanggal yang perlu diproses Modul="+LogActivityModul+" SubModul="+LogActivitySubModul+" Entity="+LogActivityEntity)
        if DBUser=='' :
            DBUser=Config.ConnLogActivity['user']
        if DBPass=='' :
            DBPass=Config.ConnLogActivity['password']
        conLogActivity = ConnectDB.connect_LogActivity(IPServer, DBUser, DBPass)
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("""select [No], DataDate, DataDateString from LogActivity.dbo.LogActivity with(nolock) where Modul='{LogActivityModul}' and SubModul='{LogActivitySubModul}' and Entity='{LogActivityEntity}' and FlagDone is NULL Order by DataDate""")
        curLogActivity.execute(qryGetTanggal.format(LogActivityModul=sanitize(LogActivityModul), LogActivitySubModul=sanitize(LogActivitySubModul), LogActivityEntity=sanitize(LogActivityEntity)))
        Tanggals = curLogActivity.fetchall()
        if len(Tanggals) > 0 :
            return {'rc' : True, 'result' : Tanggals}
        else :
            return {'rc' : False, 'msg' : str('tidak ada tanggal yang harus diproses')}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}
    finally :
        curLogActivity.close()

def updateLogActivity(IPServer, ID, FlagDone, TotalData=str('0'), DBUser=str(''), DBPass=str('')) :
    try:
        if DBUser=='' :
            DBUser=Config.ConnLogActivity['user']
        if DBPass=='' :
            DBPass=Config.ConnLogActivity['password']
        conLogActivity = ConnectDB.connect_LogActivity(IPServer, DBUser, DBPass)
        curLogActivity = conLogActivity.cursor()
        if FlagDone==str('0') :
            qryUpdateTanggal = ("""Update LogActivity.dbo.LogActivity SET StartDate=GetDate() where [No]='{ID}'""")
            curLogActivity.execute(qryUpdateTanggal.format(ID=sanitize(ID)))
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('1') :
            qryUpdateTanggal = ("""Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), TotalData='{TotalData}', FlagDone=1 where [No]='{ID}'""")
            curLogActivity.execute(qryUpdateTanggal.format(TotalData=sanitize(TotalData), ID=sanitize(ID)))
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('2') : #Error
            qryUpdateTanggal = ("""Update LogActivity.dbo.LogActivity SET EndDate=GetDate(), Note='' where [No]='{ID}'""")
            curLogActivity.execute(qryUpdateTanggal.format(sanitize(ID)))
            conLogActivity.commit()
            return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}
    finally :
        curLogActivity.close()