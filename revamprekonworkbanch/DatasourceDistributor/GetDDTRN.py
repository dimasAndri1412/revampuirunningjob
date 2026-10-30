#!/usr/bin/python
import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import ConnectDB
import threading
import os
import traceback
from DatasourceDistributorConf import getFiturCon, insertDestination, createLog
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
        sendDDTRN(self.counter)
        threadLock.release()

def getFitur(counter):
    try :
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()
        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select ID, IDFitur, FieldSelect, Rekening, WhereCondition, TableDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity 
						from dbo.ParamDDTRN with(nolock) 
						where isActive=1 and IDFitur=1200 and IDFitur% {TotalThread} = {counter} and SchStart<Convert(time, getDate())""")
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
						IF object_id('tempdb..#RekGetDDTRN') is not null 
							DROP TABLE #RekGetDDTRN;
						ELSE 
							CREATE TABLE #RekGetDDTRN(NomorRekeningYangMauDiTarik decimal(19,0) NULL);
						"""
        curDWH.execute(qryCreateTbl)
        conDWH.commit()
        for rek in Rekenings.split(",") :
            if len(rek.strip())>0 :
                sqlInsert = "Insert into #RekGetDDTRN (NomorRekeningYangMauDiTarik) values ('{rekenings}')"
                curDWH.execute(sqlInsert.format(rekenings=sanitize(rek)))
                conDWH.commit()
        return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : message}

def getDDTRN(YYYYMMDD, Rekenings, FieldSelect, WhereCondition) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil data DDTRN tanggal "+ str(YYYYMMDD) +" untuk Rekening="+Rekenings)
        if len(WhereCondition)==0 :
            WhereCondition = '1=1'
        conDWH = ConnectDB.connect_RCDWH()
        curDWH = conDWH.cursor()

        DBNameDWH = '8000_RC_YYYYMMDD'
        qrySetDBNameDWH = ("""SELECT ISNULL(MAX([name]), '8000_RC_YYYYMMDD') as dbName 
							FROM master.dbo.sysdatabases with(nolock)
							WHERE [name] in ('{DBName}')""")
        curDWH.execute(qrySetDBNameDWH.format(DBName=sanitize('8000_RC_'+str(YYYYMMDD))))
        SetDBNameDWH = curDWH.fetchone()

        TmpTbl = createTempTbl(conDWH, curDWH, Rekenings)
        if TmpTbl['rc']==False :
            return {'rc' : False, 'msg' : 'gagal membentuk temp tabel rekening  di DWH '+ str(TmpTbl['msg'])}

        Rekening = Rekenings.split(',')
        qrySelect = []
        for rek in Rekening :
            TableName = "AS4_DDTRN2_"+("00"+str(int((("0000"+rek.lstrip())[-15:])[0:4])%30))[-2:]
            if len(SetDBNameDWH) > 0 :
                if SetDBNameDWH[0]=='8000_RC_'+str(YYYYMMDD) :
                    DBNameDWH = '8000_RC_'+str(YYYYMMDD)
                else :
                    DBNameDWH = "8000_RC_YYYYMMDD"
            if DBNameDWH!="8000_RC_YYYYMMDD" and len(DBNameDWH)==16 :
                qryString = ("""Select {FieldSelect} 
								From [{DBNameDWH}].dbo.{TableName} a with(nolock) 
								inner join #RekGetDDTRN B 
									on A.TRACCT=B.NomorRekeningYangMauDiTarik 
								Where ({WhereCondition})""")
                if (qryString not in qrySelect) :
                    qrySelect.append(qryString)

        qrySelectDWH=""
        if len(qrySelect)>0 :
            qrySelectDWH = " Union All ".join(qrySelect)

        # get Data
        if len(qrySelectDWH)>0 :
            curDWH.execute(qrySelectDWH.format(FieldSelect=sanitize(FieldSelect), DBNameDWH=sanitize(DBNameDWH), TableName=sanitize(TableName), WhereCondition=sanitize(WhereCondition) ))

            # build Header
            if len(curDWH.description)>0 :
                colNameList = []
                colNames = str('')
                for i in range(len(curDWH.description)):
                    desc = curDWH.description[i]
                    colNameList.append(str('[')+desc[0]+str(']'))
                return {'rc' : True, 'result' : curDWH, 'header' : colNameList, 'count' : curDWH.rowcount}
        else :
            return {'rc' : False, 'msg' : str('cek Database DWH')}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}

def sendDDTRN(counter):
    # Get registered Fitur
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0 :
        for dataFitur in Fiturs :
            try:
                ID, IDFitur, FieldSelect, Rekening, WhereCondition, TableDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity = dataFitur
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
                            #Grab DDTRN
                            dataTransfer = getDDTRN(YYYYMMDD, Rekening, FieldSelect, WhereCondition)
                            if dataTransfer['rc']:
                                #Insert DDTRN
                                InsertData = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableDestination, dataTransfer['header'], dataTransfer['result'])
                                #Update EndDate
                                if InsertData['rc']:
                                    updateLogActivity(str(IPServer), str(No), str('1'), str(InsertData['result']))
                                    createLog(str('getDDTRN'), ID, str("Sukses insert data DDTRN tanggal ")+ str(YYYYMMDD))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal insert data DDTRN tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                    createLog(str('getDDTRN'), ID, str("Gagal insert data DDTRN tanggal ") + str(YYYYMMDD) + str(' : ')+ InsertData['result'])
                            else:
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal mendapatkan DDTRN tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                createLog(str('getDDTRN'), ID, str("Gagal insert data DDTRN tanggal ") + str(YYYYMMDD) + str(' : ')+ dataTransfer['msg'])
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses semua Tanggal untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ada Tanggal yang perlu diproses untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Parameter koneksi untuk Param "+ str(ID) +" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    createLog(str('getDDTRN'), ID, str('Parameter koneksi tidak ditemukan'))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ditemukan Fitur yang terdaftar")
        createLog(str('getDDTRN'), 0, str("tidak ditemukan Fitur yang terdaftar"))

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