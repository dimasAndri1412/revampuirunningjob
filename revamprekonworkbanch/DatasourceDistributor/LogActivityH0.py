#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
from datetime import date, datetime
from Etc import sanitize

def cekLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDate,SCHRun, DBUser=str(''), DBPass=str('')) :
    try:
        if DBUser=='' :
            DBUser=Config.ConnLogActivity['user']
        if DBPass=='' :
            DBPass=Config.ConnLogActivity['password']
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(DBUser)+";PWD="+str(DBPass))
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select [No], DataDate, DataDateString, StartDate, EndDate, TotalData, FlagDone, Note from LogActivityH0.dbo.LogActivityH0 with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and DataDate='"+str(LogActivityDate)+"' and SCHRun='"+str(SCHRun)+"' ")

        curLogActivity.execute(qryGetTanggal)
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
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(DBUser)+";PWD="+str(DBPass))
        curLogActivity = conLogActivity.cursor()
        qryGetTanggal = ("select TOP 1 [No], DataDate, DataDateString,StartTimeData,EndTimeData,cast(DataDate as datetime) + cast(SCHRun as datetime) as SCHRun,LastTimeData from LogActivityH0.dbo.LogActivityH0 with(nolock) where Modul='"+str(LogActivityModul)+"' and SubModul='"+str(LogActivitySubModul)+"' and Entity='"+str(LogActivityEntity)+"' and FlagDone is NULL and LastTimeData IS NOT NULL Order by DataDate,StartTimeData")
        curLogActivity.execute(qryGetTanggal)
        # print(qryGetTanggal)
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

def updateLogActivity(IPServer, ID, FlagDone,LastTimeValue, TotalData=str('0'), DBUser=str(''), DBPass=str(''),) :
    try:
        if DBUser=='' :
            DBUser=Config.ConnLogActivity['user']
        if DBPass=='' :
            DBPass=Config.ConnLogActivity['password']
        conLogActivity = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(Config.ConnLogActivity['database'])+";UID="+str(DBUser)+";PWD="+str(DBPass))
        curLogActivity = conLogActivity.cursor()
        if FlagDone==str('0') :
            qryUpdateTanggal = ("""Update LogActivityH0.dbo.LogActivityH0 SET StartDate=GetDate() where [No]='{ID}'""")
            curLogActivity.execute(qryUpdateTanggal.format(ID=sanitize(ID)))
            conLogActivity.commit()
            return {'rc' : True}
        elif FlagDone==str('1') :
            qryUpdateTanggal = ("""Update LogActivityH0.dbo.LogActivityH0 SET EndDate=GetDate(), TotalData='{TotalData}', FlagDone=1 where [No]='{ID}'""")
            curLogActivity.execute(qryUpdateTanggal.format(TotalData=sanitize(TotalData), ID=sanitize(ID)))

            qryUpdateLogNext = """ UPDATE LogActivityH0.dbo.LogActivityH0
					SET LastTimeData = '{LastTimeValue}'
					WHERE [No] IN ( 
						select top 1 a.[No] FROM LogActivityH0.dbo.LogActivityH0 a INNER JOIN (
						select Modul,SubModul,Entity,StartTimeData,LastTimeData FROM LogActivityH0.dbo.LogActivityH0 where [No] = '{No}'
						) b ON a.Modul = b.Modul and a.SubModul = b.SubModul AND a.Entity = b.Entity AND a.StartTimeData > b.StartTimeData WHERE a.FlagDone IS NULL
					)""".format(LastTimeValue=LastTimeValue,No=ID)
            curLogActivity.execute(qryUpdateLogNext)

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