#!/usr/bin/python
import Config
import ConnectDB
import sys
from datetime import timedelta, date, datetime
import pyodbc
import threading
import os
import traceback
from DatasourceDistributorConf import getFiturCon, insertDestination, createLog
from LogActivity import getLogActivity, updateLogActivity, cekLogActivity
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
        sendTLLOG(self.counter)
        threadLock.release()

def getFitur(counter):
    try :
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select 
							ID, IDFitur, FieldSelect, WhereCondition, TableDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity 
						from dbo.ParamTLLOG with(nolock) 
						where IDFitur=1 and ID = 1 and isActive=1 and IDFitur % {TotalThread} = {counter}""")
        curDataSourceDistributor.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataSourceDistributor.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

def getTLLOG(YYYYMMDD, FieldSelect, WhereCondition) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil data TLLOG tanggal "+ str(YYYYMMDD))
        if len(WhereCondition)==0 :
            WhereCondition = '1=1'
        conn = ConnectDB.connect_TLLOG()
        curr = conn.cursor()
        # Cek Data Exist
        CekLogTLLOG = cekLogActivity(Config.ConnTLLOG['host'], 'STAGING_BRINETS', 'GetTllogbak', '', YYYYMMDD, Config.ConnTLLOG['user'], Config.ConnTLLOG['password'])
        if CekLogTLLOG['rc'] :
            for cek in CekLogTLLOG['result'] :
                YYYYMMDD = cek[2]
                FlagDone = cek[6]
            if str(FlagDone)=='1' :
                qrySetDBName = ("""SELECT ISNULL(MAX([name]), 'STAGING_BRINETS_YYYYMMDD') as dbName 
								FROM master.dbo.sysdatabases with(nolock)
								WHERE [name]='{DBName}' """)
                curr.execute(qrySetDBName.format(DBName=sanitize('STAGING_BRINETS_'+YYYYMMDD)))
                SetDBName = curr.fetchone()
                if len(SetDBName) > 0 :
                    if SetDBName[0]=='STAGING_BRINETS_'+YYYYMMDD :
                        DBName = 'STAGING_BRINETS_'+YYYYMMDD
                    else :
                        DBName = "STAGING_BRINETS_YYYYMMDD"

                if DBName!="STAGING_BRINETS_YYYYMMDD" :
                    qrySelect = """Select {FieldSelect} 
								From [{DBName}].dbo.AS4_TLLOGBAK with(nolock) 
								Where ({WhereCondition})"""
                elif DBName=="STAGING_BRINETS_YYYYMMDD" :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> DB STAGING_BRINETS tanggal "+str(YYYYMMDD)+" tidak ada")
                    qrySelect = ""
                else :
                    qrySelect = ""
                # get Data
                if len(qrySelect)>0 :
                    curr.execute(qrySelect.format(FieldSelect=sanitize(FieldSelect), DBName=sanitize(DBName), WhereCondition=sanitize(WhereCondition)))
                    # build Header
                    if len(curr.description)>0 :
                        colNameList = []
                        colNames = str('')
                        for i in range(len(curr.description)):
                            desc = curr.description[i]
                            colNameList.append(str('[')+desc[0]+str(']'))
                        return {'rc' : True, 'result' : curr, 'header' : colNameList}
                else :
                    return {'rc' : False, 'msg' : 'Gagal mendapatkan data TLLOG, cek DB STAGING_BRINETS tanggal '+str(YYYYMMDD)}
            else :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> STAGING_BRINETS tanggal "+str(YYYYMMDD)+" belum selesai")
                return {'rc' : False, 'msg' : 'Staging TLLOG tanggal '+str(YYYYMMDD)+' belum selesai'}
        else :
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> STAGING_BRINETS tanggal "+str(YYYYMMDD)+" tidak ada di LogActivity")
            return {'rc' : False, 'msg' : str('cek LogActivity Staging Tllog : ')+CekLogTLLOG['msg']}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : 'error get Tllog from STAGING_BRINETS : '+str(message)}

def sendTLLOG(counter):
    # Get registered Fitur
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0 :
        for dataFitur in Fiturs :
            try :
                ID, IDFitur, FieldSelect, WhereCondition, TableDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity = dataFitur
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
                            #Grab TLLOG
                            dataTransfer = getTLLOG(YYYYMMDD, FieldSelect, WhereCondition)
                            if dataTransfer['rc']:
                                #Insert TLLOG
                                InsertData = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableDestination, dataTransfer['header'], dataTransfer['result'])
                                if InsertData['rc']:
                                    #Update EndDate
                                    updateLogActivity(str(IPServer), str(No), str('1'), str(InsertData['result']))
                                    createLog(str('getTLLOG'), ID, str("Sukses insert data TLLOG tanggal ")+ str(YYYYMMDD))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal insert data TLLOG tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                    createLog(str('getTLLOG'), ID, str("Gagal insert data TLLOG tanggal ")+ str(YYYYMMDD) + str(' : ') +  str(InsertData['result']))
                            else:
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal mendapatkan TLLOG tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                createLog(str('getTLLOG'), ID, str("Gagal insert data TLLOG tanggal ") + str(YYYYMMDD) + str(' : ')+ dataTransfer['msg'])
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses semua Tanggal untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ada Tanggal yang perlu diproses untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Parameter koneksi untuk Param "+ str(ID) +" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    createLog(str('getTLLOG'), ID, str('Parameter koneksi tidak ditemukan'))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ditemukan Fitur yang terdaftar")
        createLog(str('getTLLOG'), 0, str("tidak ditemukan Fitur yang terdaftar"))

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