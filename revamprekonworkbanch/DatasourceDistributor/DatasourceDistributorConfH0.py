#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
import pandas as pd
from datetime import timedelta, date, datetime
from Etc import  checkColumnExist, sanitize, passDecrypt


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


def insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableTrueDestination, header, dataInsert,LastTimeData):
    try:
        if DBType=='mssql' :
            conDataFitur = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
            conDataFitur.autocommit = False
            curDataFitur = conDataFitur.cursor()
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> validasi kolom tabel tujuan server "+str(IPServer)+" DB "+str(DBName))
            ColumnExist = checkColumnExist(curDataFitur, TableTrueDestination, header)

            if ColumnExist :
                colNames = ','.join(header)
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Insert data")

                fieldsCursor = getFieldsIndex(dataInsert)
                qryInsertDestination = ("""Insert into {TableTrueDestination} ({colNames}) values ("""+ ','.join(['?'] * len(header)) + """)""")

                if isinstance(dataInsert, pd.DataFrame):
                    curDataFitur.fast_executemany = False
                    rowdata = row_data(dataInsert.values.tolist(),fieldsCursor)
                    LastTimeValue = LastTimeData if len(rowdata) == 0 else str(datetime.strptime(rowdata[1][-1],'%H:%M:%S')+timedelta(0,1))
                    if rowdata :
                        curDataFitur.executemany(qryInsertDestination.format(TableTrueDestination=sanitize(TableTrueDestination), colNames=sanitize(colNames)), rowdata[0])

                else :
                    curDataFitur.fast_executemany = True
                    rowdata = row_data(dataInsert,fieldsCursor)
                    LastTimeValue = LastTimeData if len(rowdata) == 0 else str(datetime.strptime(rowdata[1][-1],'%H:%M:%S')+timedelta(0,1))
                    # print(rowdata[0])
                    if rowdata :
                        curDataFitur.executemany(qryInsertDestination.format(TableTrueDestination=sanitize(TableTrueDestination), colNames=sanitize(colNames)), rowdata[0])

                conDataFitur.commit()
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Selesai insert data")
                return {'rc':True, 'result':str(curDataFitur.rowcount),'LastTimeValue':str(LastTimeValue)}
            else :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Gagal validasi kolom tabel tujuan")
                return {'rc':False, 'result':str('Gagal validasi kolom tabel tujuan'),'LastTimeValue':''}
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

def getFieldsIndex(cursor) :
    results = {}
    column = 0
    for d in cursor.description :
        results[d[0]] = column
        column = column +1

    return results

def row_data(cursor,fieldsCursor, some=Config.batchbuffer):
    try:
        fetch = cursor.fetchmany
        data = []
        while True:
            rowx = []
            rowtime = []
            rows = fetch(some)
            if not rows: break
            for row in rows:
                # print(row[fieldsCursor['TLBTMIConvertTime']])
                rowtime.append(row[fieldsCursor['TLBTMIConvertTime']])
                listr = list(row)
                del listr[-1]
                # print(rowtime)
                rowx.append(tuple(listr))
            data = [rowx,rowtime]
        return data

    except Exception as e:
        message = traceback.format_exc()
        print(message)
