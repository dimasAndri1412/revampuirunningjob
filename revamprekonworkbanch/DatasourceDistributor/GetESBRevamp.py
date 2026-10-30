import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import ConnectDB
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
        sendESB(self.counter)
        threadLock.release()

def getFitur(counter):
    try :
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select 
							ID, IDFitur, FieldSelect, WhereCondition, TableDestination, LogActivityModul, LogActivitySubModul, LogActivityEntity 
						from dbo.ParamESBRevamp with(nolock) 
						where isActive=1 and IDFitur = 1 and IDFitur%{TotalThread} = {counter} and SchStart<Convert(time, getDate()) """)
        curDataSourceDistributor.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataSourceDistributor.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

def getESB(YYYYMMDD, FieldSelect, WhereCondition) :
    try:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> mengambil data ESB tanggal "+ str(YYYYMMDD))
        if len(WhereCondition)==0 :
            WhereCondition = '1=1'
        conn = ConnectDB.connect_ESBRevamp()
        curr = conn.cursor()
        # Cek Data Exist
        qryCekLogActivity = ("""
							declare @cekFlagDone as tinyint;
							IF Convert(varchar(10), GetDate(), 121)='{YYYY_MM_DD}'
							BEGIN
								SELECT 'StagingESB' as DBName
							END
							ELSE
							BEGIN
								IF NOT EXISTS(select * from LogActivity..LogActivity with(nolock) where Task ='StagingESB' and Activity='BackupData' and DataDate='{YYYY_MM_DD}')
								BEGIN
									SELECT 'GaReadyBro' as DBName
								END
							ELSE
							BEGIN
								SELECT @cekFlagDone=ISNULL(FlagDone,0) 
								FROM LogActivity..LogActivity with(nolock) where Task ='StagingESB' and Activity ='BackupData' and DataDate='{YYYY_MM_DD}'
								IF @cekFlagDone=0
									BEGIN
										SELECT 'GaReadyBro' as DBName
									END
									ELSE
									BEGIN
										SELECT 'StagingESB_'+convert(varchar(10),convert(date,'{YYYY_MM_DD}'),112) as DBName
									END
								END
							END
							""")
        print(qryCekLogActivity.format(YYYY_MM_DD=sanitize(str(YYYYMMDD))))
        curr.execute(qryCekLogActivity.format(YYYY_MM_DD=sanitize(str(YYYYMMDD))))
        GetDBName = curr.fetchall()
        DBName = GetDBName[0][0]

        if DBName!='GaReadyBro' :
            qryCekDB = ("""SELECT count(1) as cek 
							FROM master.dbo.sysdatabases with(nolock) 
							WHERE [name]='{DBName}' """)

            curr.execute(qryCekDB.format(DBName=sanitize(DBName)))
            cekDB = curr.fetchone()
            if int(cekDB[0]) > 0 :
                qrySelect = """Select {FieldSelect} 
								From [{DBName}].dbo.StagingESB with(nolock) 
								Where ({WhereCondition})"""
            else :
                qrySelect = ""
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> DB "+str(DBName)+" tidak ada")
                return {'rc' : False, 'msg' : str("DB ")+str(DBName)+str(" tidak ada")}
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
                    return {'rc' : False, 'msg' : 'Error mengambil deskripsi kolom'}
        else :
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> DB LOG ESB tanggal "+str(YYYYMMDD)+" tidak ada di LogActivity")
            return {'rc' : False, 'msg' : str("LogActivity ESB tanggal ")+str(YYYYMMDD)+(" tidak ditemukan")}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : str(message)}

def sendESB(counter):
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
                            YYYY_MM_DD = dataTanggal[1].strftime("%Y-%m-%d")
                            #Update StartDate
                            updateLogActivity(str(IPServer), str(No), str(0))
                            #Grab ESB
                            dataTransfer = getESB(YYYY_MM_DD, FieldSelect, WhereCondition)
                            if dataTransfer['rc']:
                                #Insert ESB
                                InsertData = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableDestination, dataTransfer['header'], dataTransfer['result'])
                                if InsertData['rc']:
                                    #Update EndDate
                                    updateLogActivity(str(IPServer), str(No), str('1'), str(InsertData['result']))
                                    createLog(str('getESB'), ID, str("Sukses insert data ESB tanggal ")+ str(YYYYMMDD))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal insert data ESB tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                    createLog(str('getESB'), ID, str("Gagal insert data ESB tanggal ") + str(YYYYMMDD) + str(' : ')+ InsertData['result'])
                            else:
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Gagal mendapatkan ESB tanggal "+ str(YYYYMMDD) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                createLog(str('getESB'), ID, str("Gagal insert data ESB tanggal ") + str(YYYYMMDD) + str(' : ')+ dataTransfer['msg'])
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses semua Tanggal untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ada Tanggal yang perlu diproses untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Parameter koneksi untuk Param "+ str(ID) +" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    createLog(str('getESB'), ID, str('Parameter koneksi tidak ditemukan'))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ditemukan Fitur yang terdaftar")
        createLog(str('getESB'), 0, str("tidak ditemukan Fitur yang terdaftar"))

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