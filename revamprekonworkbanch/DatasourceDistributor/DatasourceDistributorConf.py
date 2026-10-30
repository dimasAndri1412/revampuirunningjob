#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
import pandas as pd
from datetime import timedelta, date, datetime
from Etc import row_gen, checkColumnExist, sanitize, passDecrypt


def getFiturCon(IDFitur) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil Parameter koneksi untuk Fitur "+ str(IDFitur))

        conDataSourceDistributor = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnDataSourceDistributor['host'])+";DATABASE="+str(Config.ConnDataSourceDistributor['database'])+";UID="+str(Config.ConnDataSourceDistributor['user'])+";PWD="+str(Config.ConnDataSourceDistributor['password']))
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        qryGetConFitur = ("""select 
								IPServer, DBName, DBUser, DBPass, DBType 
							from ParamFitur with(nolock) 
							where IDFitur='{IDFitur}'""")
        curDataSourceDistributor.execute(qryGetConFitur.format(IDFitur=int(sanitize(IDFitur))))
        ConFiturs = curDataSourceDistributor.fetchone()
        if len(ConFiturs) > 0 :
            return {'rc' : True, 'result' : ConFiturs}
        else :
            return {'rc' : False, 'msg' : str('configurasi koneksi tidak ditemukan')}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}
    finally :
        curDataSourceDistributor.close()

def insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableTrueDestination, header, dataInsert):
    try:
        if DBType=='mssql' :
            conDataFitur = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
            conDataFitur.autocommit = False
            curDataFitur = conDataFitur.cursor()
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> validasi kolom tabel tujuan server "+str(IPServer)+" DB "+str(DBName))

            ColumnExist = checkColumnExist(curDataFitur, TableTrueDestination, header)
            #print(header)
            if ColumnExist :
                colNames = ','.join(header)
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Insert data")
                qryInsertDestination = ("""Insert into {TableTrueDestination} ({colNames}) values ("""+ ','.join(['?'] * len(header)) + """)""")
                if isinstance(dataInsert, pd.DataFrame):
                    curDataFitur.fast_executemany = False
                    curDataFitur.executemany(qryInsertDestination.format(TableTrueDestination=sanitize(TableTrueDestination), colNames=sanitize(colNames)), dataInsert.values.tolist())
                else :
                    curDataFitur.fast_executemany = True
                    curDataFitur.executemany(qryInsertDestination.format(TableTrueDestination=sanitize(TableTrueDestination), colNames=sanitize(colNames)), row_gen(dataInsert))
                conDataFitur.commit()
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Selesai insert data")
                return {'rc':True, 'result':str(curDataFitur.rowcount)}
            else :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Gagal validasi kolom tabel tujuan")
                return {'rc':False, 'result':str('Gagal validasi kolom tabel tujuan')}
            curDataFitur.close()
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        conDataFitur.rollback()
        return {'rc':False, 'result': str(message)}
    finally :
        curDataFitur.close()


def createLog(Task, IDTask, Note) :
    try:
        conDataSourceDistributor = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnDataSourceDistributor['host'])+";DATABASE="+str(Config.ConnDataSourceDistributor['database'])+";UID="+str(Config.ConnDataSourceDistributor['user'])+";PWD="+str(Config.ConnDataSourceDistributor['password']))
        conDataSourceDistributor.autocommit = True
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        qryGetConFitur = ("Insert into LogExecute (DateExec, Task, IDTask, Note) values (GetDate(), ?, ?, ?)")
        curDataSourceDistributor.execute(qryGetConFitur, (Task, IDTask,Note))
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        curDataSourceDistributor.close()