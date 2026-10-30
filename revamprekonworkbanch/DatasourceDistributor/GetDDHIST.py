#!/usr/bin/python
import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import ConnectDB
import threading
import os
import traceback
from DataSourceDistributorConf import getFiturCon, insertDestination, createLog
from LogActivity import getLogActivity, updateLogActivity
from Etc import sanitize

class DistThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter=counter
    def run(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        sendDDHIST(self.counter)
        threadLock.release()

def getFitur(counter):
    try :
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()
        # Get registered Fitur
        # print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = """select 
							ID, IDFitur, FieldSelectTrue, FieldSelectFalse, Rekening, ISNULL(WhereCondition, '') WhereCondition, TableTrueDestination, TableFalseDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity 
						from dbo.ParamDDHIST a with(nolock) 
						where isActive=1 and IDFitur = 1 and IDFitur % {TotalThread} = {counter}"""
        curDataSourceDistributor.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataSourceDistributor.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

def createTempTbl(conDWH, curDWH, Rekenings):
    try:
        qryCreateTbl = """SET NOCOUNT ON;
						IF object_id('tempdb..#RekGetDDHIST') is not null 
							DROP TABLE #RekGetDDHIST;
						ELSE 
							CREATE TABLE #RekGetDDHIST(NomorRekeningYangMauDiTarik decimal(19,0) NULL);
						"""
        curDWH.execute(qryCreateTbl)
        conDWH.commit()
        for rek in Rekenings.split(",") :
            if len(rek.strip())>0 :
                sqlInsert = "Insert into #RekGetDDHIST (NomorRekeningYangMauDiTarik) values ('{rekenings}')"
                curDWH.execute(sqlInsert.format(rekenings=sanitize(rek)))
                conDWH.commit()
        return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : message}

def getDDHIST(YYYYMMDD, Rekening, FieldSelect, WhereCondition, TrueFalse) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil data DDHIST tanggal "+ str(YYYYMMDD) +" untuk Rekening="+Rekening)
        if len(WhereCondition)==0 :
            WhereCondition = '1=1'
        conDWH = ConnectDB.connect_RCDWH()
        curDWH = conDWH.cursor()
        # Cek Data Exist
        qryCekLogDDHIST = ("""select ISNULL(MAX(TOTAL), 0) as total 
								from [dbo].[TBL_LOG_DDDHIS] with(nolock) 
								Where POSITION=[{DatabaseName}].dbo.TO_JULIANDATE('{YYYYMMDD}')""")
        curDWH.execute(qryCekLogDDHIST.format(DatabaseName=sanitize(Config.ConnRCDWH['database']), YYYYMMDD=sanitize(YYYYMMDD)))
        print(qryCekLogDDHIST.format(DatabaseName=sanitize(Config.ConnRCDWH['database']), YYYYMMDD=sanitize(YYYYMMDD)))
        cekTotalDWH = curDWH.fetchone()
        TotalDWH = int(cekTotalDWH[0])

        qrySetDBNameDWH = ("""SELECT ISNULL(MAX([name]), '8000_RC_YYYYMMDD') as dbName 
								FROM master.dbo.sysdatabases with(nolock) 
								WHERE [name]='{DBDaily}' or [name]='{DBMonthly}'""")
        curDWH.execute(qrySetDBNameDWH.format(DBDaily=sanitize(str("8000_RC_")+str(YYYYMMDD)), DBMonthly=sanitize(str("8000_RC_")+str(YYYYMMDD[:-2]))))

        SetDBNameDWH = curDWH.fetchone()
        if TrueFalse :
            negation = str("")
        else :
            negation = str(" not ")

        if len(Rekening) > 0 : ## len rek > 0
            #edit if disini
            TmpTbl = createTempTbl(conDWH, curDWH, Rekening)
            if TmpTbl['rc']==False :
                return {'rc' : False, 'msg' : 'gagal membentuk temp tabel rekening  di DWH '+ str(TmpTbl['msg'])}

            if len(SetDBNameDWH) > 0 :
                if SetDBNameDWH[0]!="8000_RC_YYYYMMDD" and TotalDWH>int(10) and SetDBNameDWH[0]==str("8000_RC_")+str(YYYYMMDD) :
                    DBNameDWH = str("8000_RC_")+str(YYYYMMDD)
                    qrySelectDWH = """Select {FieldSelect} 
										From [{DBNameDWH}].dbo.AS4_DDDHIS A with(nolock) 
											inner join #RekGetDDHIST B 
										on A.TRACCT=B.NomorRekeningYangMauDiTarik 
										Where TRDATE=[{DBMaster}].dbo.TO_JULIANDATE('{YYYYMMDD}') 
											and {negation} ({WhereCondition})"""
                elif SetDBNameDWH[0]!="8000_RC_YYYYMMDD" and TotalDWH>int(10) and SetDBNameDWH[0]==str("8000_RC_")+str(YYYYMMDD[:-2]) :
                    DBNameDWH = str("8000_RC_")+str(YYYYMMDD[:-2])
                    qrySelectDWH = """Select {FieldSelect} 
										From (
											Select *, TRTIME as TIMENT, TRDORC as DORC 
											From [{DBNameDWH}].dbo.AS4_DDHIST A with(nolock) 
												inner join #RekGetDDHIST B 
											on A.TRACCT=B.NomorRekeningYangMauDiTarik 
											Where TRDATE=[{DBMaster}].dbo.TO_JULIANDATE('{YYYYMMDD}')) as A 
										Where {negation} ({WhereCondition})"""
                elif SetDBNameDWH[0]=="8000_RC_YYYYMMDD" :
                    DBNameDWH = '8000_RC_YYYYMMDD'
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Database 8000_RC_"+ str(YYYYMMDD) +" atau 8000_RC_"+ str(YYYYMMDD[:-2]) +" tidak ada")
                    qrySelectDWH = ""
                elif TotalDWH<=int(10) :
                    DBNameDWH = '8000_RC_YYYYMMDD'
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Log DDHIST tanggal "+str(YYYYMMDD)+" tidak valid")
                    qrySelectDWH = ""
                else :
                    qrySelectDWH = ""
            else :
                DBNameDWH = '8000_RC_YYYYMMDD'
            print(qrySelectDWH.format(FieldSelect=sanitize(FieldSelect), DBNameDWH=sanitize(DBNameDWH), DBMaster=sanitize(Config.ConnRCDWH['database']), YYYYMMDD=sanitize(YYYYMMDD), negation=negation, WhereCondition=sanitize(WhereCondition)))
        else :## len rek < 0
            if len(SetDBNameDWH) > 0 :
                if SetDBNameDWH[0]!="8000_RC_YYYYMMDD" and TotalDWH>int(10) and SetDBNameDWH[0]==str("8000_RC_")+str(YYYYMMDD) :
                    DBNameDWH = str("8000_RC_")+str(YYYYMMDD)
                    qrySelectDWH = """Select {FieldSelect} 
										From [{DBNameDWH}].dbo.AS4_DDDHIS with(nolock) 
										Where TRDATE=[{DBMaster}].dbo.TO_JULIANDATE('{YYYYMMDD}') 
											and {negation} ({WhereCondition})"""
                elif SetDBNameDWH[0]!="8000_RC_YYYYMMDD" and TotalDWH>int(10) and SetDBNameDWH[0]==str("8000_RC_")+str(YYYYMMDD[:-2]) :
                    DBNameDWH = str("8000_RC_")+str(YYYYMMDD[:-2])
                    qrySelectDWH = """Select {FieldSelect} 
										From (
											Select *, TRTIME as TIMENT, TRDORC as DORC 
											From [{DBNameDWH}].dbo.AS4_DDHIST with(nolock) 
											Where TRDATE=[{DBMaster}].dbo.TO_JULIANDATE('{YYYYMMDD}')) as A 
										Where {negation} ({WhereCondition})"""
                elif SetDBNameDWH[0]=="8000_RC_YYYYMMDD" :
                    DBNameDWH = '8000_RC_YYYYMMDD'
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Database 8000_RC_"+ str(YYYYMMDD) +" atau 8000_RC_"+ str(YYYYMMDD[:-2]) +" tidak ada")
                    qrySelectDWH = ""
                elif TotalDWH<=int(10) :
                    DBNameDWH = '8000_RC_YYYYMMDD'
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Log DDHIST tanggal "+str(YYYYMMDD)+" tidak valid")
                    qrySelectDWH = ""
                else :
                    qrySelectDWH = ""
            else :
                DBNameDWH = '8000_RC_YYYYMMDD'
            # print(qrySelectDWH)
            print(qrySelectDWH.format(FieldSelect=sanitize(FieldSelect), DBNameDWH=sanitize(DBNameDWH), DBMaster=sanitize(Config.ConnRCDWH['database']), YYYYMMDD=sanitize(YYYYMMDD), negation=negation, WhereCondition=sanitize(WhereCondition)))

        # get Data
        if len(qrySelectDWH)>0 :
            curDWH.execute(qrySelectDWH.format(FieldSelect=sanitize(FieldSelect), DBNameDWH=sanitize(DBNameDWH), DBMaster=sanitize(Config.ConnRCDWH['database']), YYYYMMDD=sanitize(YYYYMMDD), negation=negation, WhereCondition=sanitize(WhereCondition)))
            # build Header
            if len(curDWH.description)>0 :
                colNameList = []
                colNames = str('')
                for i in range(len(curDWH.description)):
                    desc = curDWH.description[i]
                    colNameList.append(str('[')+desc[0]+str(']'))
                return {'rc' : True, 'result' : curDWH, 'header' : colNameList, 'count' : curDWH.rowcount}
        else :
            return {'rc' : False, 'msg' : 'Gagal mendapatkan data DDHIST dari DWH tanggal '+str(YYYYMMDD)+', cek DB atau Log DDHIST di DWH'}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : message}

def sendDDHIST(counter):
    # Get registered Fitur
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0 :
        for dataFitur in Fiturs :
            try:
                ID, IDFitur, FieldSelectTrue, FieldSelectFalse, Rekening, WhereCondition, TableTrueDestination, TableFalseDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity = dataFitur
                #Get Fitur Connection
                FiturCon = getFiturCon(IDFitur)
                if FiturCon['rc'] :
                    IPServer, DBName, DBUser, DBPass, DBType = FiturCon['result']
                    #Get LogActivity
                    Tanggals = getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)
                    if Tanggals['rc'] :
                        for dataTanggal in Tanggals['result'] :
                            No = dataTanggal[0]
                            YYYYMMDD = dataTanggal[1].strftime("%Y%m%d")
                            #Update StartDate
                            updateLogActivity(str(IPServer), str(No), str(0))
                            #Grab DDHIST True
                            dataTransferTrue = getDDHIST(YYYYMMDD, Rekening, FieldSelectTrue, WhereCondition, True)
                            #print(dataTransferTrue)
                            if dataTransferTrue['rc']:
                                #Insert DDHIST
                                InsertDataTrue = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableTrueDestination, dataTransferTrue['header'], dataTransferTrue['result'])
                                #print(InsertDataTrue)
                                #Update EndDate
                                if InsertDataTrue['rc']:
                                    updateLogActivity(str(IPServer), str(No), str('1'), str(dataTransferTrue['count']))
                                    createLog(str('getDDHIST'), ID, str("Sukses insert data DDHIST tanggal ")+ str(YYYYMMDD))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal insert data DDHIST tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                    createLog(str('getDDHIST'), ID, str("Gagal insert data DDHIST tanggal ")+ str(YYYYMMDD) + str(' : ') + str(InsertDataTrue['result']))
                            else:
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal mendapatkan DDHIST tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                createLog(str('getDDHIST'), ID, str("Gagal insert data DDHIST tanggal ")+ str(YYYYMMDD) + str(' : ') + str(dataTransferTrue['msg']))
                            #Grab DDHIST False
                            if len(TableFalseDestination)>0 and len(FieldSelectFalse)>0 :
                                dataTransferFalse = getDDHIST(YYYYMMDD, Rekening, FieldSelectFalse, WhereCondition, False)
                                if dataTransferFalse['rc']:
                                    #Insert DDHIST
                                    InsertDataFalse = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableFalseDestination, dataTransferFalse['header'], dataTransferFalse['result'])
                                    if InsertDataFalse['rc']:
                                        createLog(str('getDDHIST'), ID, str("Sukses insert data DDHIST False Condition tanggal ") + str(YYYYMMDD))
                                    else :
                                        createLog(str('getDDHIST'), ID, str("Gagal insert data DDHIST False Condition tanggal ")+ str(YYYYMMDD) + str(' : ') + str(InsertDataFalse['result']))
                                else:
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal mendapatkan DDHIST False Destination tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                    createLog(str('getDDHIST'), ID, str("Gagal insert data DDHIST False Condition tanggal ")+ str(YYYYMMDD) + str(' : ') + str(dataTransferFalse['msg']))

                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses semua Tanggal untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ada Tanggal yang perlu diproses untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Parameter koneksi untuk Param "+ str(ID) +" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    createLog(str('getDDHIST'), ID, str('Parameter koneksi tidak ditemukan'))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ditemukan Fitur yang terdaftar")
        createLog(str('getDDHIST'), 0, str("tidak ditemukan Fitur yang terdaftar"))

def main():
    try:
        threads = []
        for x in range(Config.threads):
            th = DistThread(x, "Thread-"+str(x), x)
            th.start()
            threads.append(th)
        for t in threads:
            t.join()
    except Exception as e:
        message = traceback.format_exc()
        print(message)
if __name__ == "__main__":
    main()