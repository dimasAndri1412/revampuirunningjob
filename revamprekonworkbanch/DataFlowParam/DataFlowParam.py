import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import threading
import os
import traceback
from DataFlowParamConfig import getServerConnection,createLog,replaceDateParam
from LogActivity import getLogActivity, updateLogActivity,cekLogActivity
from Etc import sanitize
import re
from DataFlowParamBuilder import getSourceSelect,getSourceSP,insertDestination,genColumnNameListSelect,genColumnNameListSP

class DistThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter=counter
    def run(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        DataFlow(self.counter)
        threadLock.release()

def checkCriteria(IDFitur, YYYYMMDD):
    try:
        conDataFlowParam = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(Config.ConnDataFlowParam['host'])+";DATABASE="+str(Config.ConnDataFlowParam['database'])+";UID="+str(Config.ConnDataFlowParam['user'])+";PWD="+str(Config.ConnDataFlowParam['password']))
        curDataFlowParam = conDataFlowParam.cursor()

        qryFiturCekLogActivity = ("""select ID,IPServerLogActivityId,LogActivityModul,LogActivitySubModul,LogActivityEntity,LogActivityDateAdd 
									from dbo.ParamFiturCekLogActivity with(nolock) 
									where IDFitur ={IDFitur} and isActive=1 """)
        # print("test")
        # print(qryFiturCekLogActivity.format(IDFitur=int(sanitize(IDFitur))))

        curDataFlowParam.execute(qryFiturCekLogActivity.format(IDFitur=int(sanitize(IDFitur))))
        FiturCekLogActivity = curDataFlowParam.fetchall()
        CekLogActivityTrue = False
        #print(FiturCekLogActivity)

        if len(FiturCekLogActivity) > 0 :

            for CekLogActivityVar in FiturCekLogActivity :
                ID,IPServerLogActivityId,LogActivityModul,LogActivitySubModul,LogActivityEntity,LogActivityDateAdd  = CekLogActivityVar

                #print (LogActivitySubModul)
                connIPLogActivity = getServerConnection(IPServerLogActivityId,'CekLogActivity ID '+str(ID))
                #print(connIPLogActivity)
                if connIPLogActivity['rc']  :
                    #print('cekDone')
                    IPServerIPLogActivity, DBUserIPLogActivity, DBPassIPLogActivity, DBTypeIPLogActivity, DBPortLogActivity,IsListenerHALogActivity,ApplicationIntentLogActivity,InitialCatalogLogActivity = connIPLogActivity['result']

                    toDataDate = datetime.strptime(YYYYMMDD, "%Y%m%d")
                    Tanggal = toDataDate + timedelta(days=LogActivityDateAdd)

                    cekDone = cekLogActivity(IPServerIPLogActivity, DBUserIPLogActivity, DBPassIPLogActivity, ApplicationIntentLogActivity,InitialCatalogLogActivity, LogActivityModul, LogActivitySubModul, LogActivityEntity, Tanggal.strftime("%Y%m%d"))

                    if cekDone['rc'] :
                        for cek in cekDone['result'] :
                            if str(cek[6])=='1' :
                                CekLogActivityTrue = True
                            else :
                                CekLogActivityTrue = False
                                break
                    if CekLogActivityTrue==False :
                        break
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Koneksi untuk server CekLogActivity ID"+str(ID)+" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    CekLogActivityTrue = False
                    break
        else :
            CekLogActivityTrue = True
        return CekLogActivityTrue
    except Exception as e:
        return False
        message = traceback.format_exc()
        print(message)
    finally :
        curDataFlowParam.close()

def getFitur(counter):
    try :
        conDataFlowParam = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(Config.ConnDataFlowParam['host'])+";DATABASE="+str(Config.ConnDataFlowParam['database'])+";UID="+str(Config.ConnDataFlowParam['user'])+";PWD="+str(Config.ConnDataFlowParam['password']))
        curDataFlowParam = conDataFlowParam.cursor()
        # Get registered Fitur
        # print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = """select 
							IDFitur,FiturName,IPServerSourceId,DBSource,StoredProcedureExecute,TableSourceA,TableSourceB,TableSourceC,TableSourceD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,FieldSelect,WhereCondition,GroupByCondition,HavingCondition,OrderByCondition,IPServerDestinationId,DBDestination,TableDestination,IPServerLogActivityId,LogActivityModul,LogActivitySubModul,LogActivityEntity,LogActivityDateAddSource,LogActivityDateAddDestination 
						from dbo.ParamFitur a with(nolock) 
						where isActive=1 and IDFitur = 9501512 and IDFitur % {TotalThread} = {counter} """
        curDataFlowParam.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataFlowParam.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataFlowParam.close()




def DataFlow(counter):
    # Get registered Fitur
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0 :
        for dataFitur in Fiturs :
            try:
                IDFitur,FiturName,IPServerSourceId,DBSourceFormat,StoredProcedureExecuteFormat,TableSourceAFormat,TableSourceBFormat,TableSourceCFormat,TableSourceDFormat,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKeyFormat,ABJoinCKeyFormat,ABCJoinDKeyFormat,FieldSelectFormat,WhereConditionFormat,GroupByConditionFormat,HavingConditionFormat,OrderByConditionFormat,IPServerDestinationId,DBDestinationFormat,TableDestinationFormat,IPServerLogActivityId,LogActivityModul,LogActivitySubModul,LogActivityEntity,LogActivityDateAddSource,LogActivityDateAddDestination  = dataFitur
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" : Mengambil parameter untuk Fitur "+ str(IDFitur))
                #Get Fitur Connection
                FiturIPSource = getServerConnection(IPServerSourceId,'IPSource')
                FiturIPDestination = getServerConnection(IPServerDestinationId,'IPDestination')
                FiturIPLogActivity = getServerConnection(IPServerLogActivityId,'IPLogActivity')
                #print(FiturIPLogActivity)
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Done : Parameter Server terdaftar")

                if (FiturIPSource['rc'] and FiturIPDestination['rc'] and FiturIPLogActivity['rc'])  :
                    IPServerSource, DBUserSource, DBPassSource, DBTypeSource, DBPortSource, IsListenerHASource,ApplicationIntentSource,InitialCatalogSource  = FiturIPSource['result']
                    IPServerDestination, DBUserDestination, DBPassDestination, DBTypeDestination, DBPortDestination, IsListenerHADestination,ApplicationIntentDestination,InitialCatalogDestination  = FiturIPDestination['result']
                    IPServerIPLogActivity, DBUserIPLogActivity, DBPassIPLogActivity, DBTypeIPLogActivity, DBPortLogActivity, IsListenerHALogActivity,ApplicationIntentLogActivity,InitialCatalogLogActivity  = FiturIPLogActivity['result']

                    #Get LogActivity
                    Tanggals = getLogActivity(IPServerIPLogActivity, DBPortLogActivity, DBUserIPLogActivity, DBPassIPLogActivity, ApplicationIntentLogActivity,InitialCatalogLogActivity,  LogActivityModul, LogActivitySubModul, LogActivityEntity)
                    if Tanggals['rc'] :
                        for dataTanggal in Tanggals['result'] :
                            No = dataTanggal[0]
                            YYYYMMDD = dataTanggal[1].strftime("%Y%m%d")

                            #print(YYYYMMDD)
                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>> Cek : LogActivity Kriteria untuk tanggal "+str(YYYYMMDD))
                            checkCriteriaLogActivity = checkCriteria(IDFitur, YYYYMMDD)
                            #print(checkCriteriaLogActivity)

                            if checkCriteriaLogActivity :
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Done : Kriteria LogActivity terpenuhi ")
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>> DataFlow :  Start untuk IDFitur "+str(IDFitur)+" Tanggal "+str(YYYYMMDD))
                                #Update StartDate
                                updateLogActivity(IPServerIPLogActivity, DBUserIPLogActivity, DBPassIPLogActivity, ApplicationIntentLogActivity,InitialCatalogLogActivity, str(No), str(0))



                                if len(StoredProcedureExecuteFormat)>0 :
                                    #update date format
                                    DBSource = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), DBSourceFormat)
                                    FieldSelect = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), FieldSelectFormat)
                                    #getsource
                                    StoredProcedureExecute = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), StoredProcedureExecuteFormat)
                                    dataTransfer = getSourceSP(DBTypeSource, IPServerSource, DBPortSource, DBUserSource, DBPassSource,DBSource,ApplicationIntentSource,InitialCatalogSource,StoredProcedureExecute)
                                    isSPExecute = True


                                else :
                                    #update date format
                                    DBSource = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), DBSourceFormat)
                                    FieldSelect = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), FieldSelectFormat)


                                    TableSourceA = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), TableSourceAFormat)
                                    TableSourceB = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), TableSourceBFormat)
                                    TableSourceC = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), TableSourceCFormat)
                                    TableSourceD = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), TableSourceDFormat)



                                    AJoinBKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), AJoinBKeyFormat)
                                    ABJoinCKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), ABJoinCKeyFormat)
                                    ABCJoinDKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), ABCJoinDKeyFormat)


                                    WhereCondition = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), WhereConditionFormat)
                                    GroupByCondition = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), GroupByConditionFormat)
                                    HavingCondition = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), HavingConditionFormat)

                                    OrderByCondition = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddSource, m.group()), OrderByConditionFormat)
                                    #getsource
                                    dataTransfer = getSourceSelect(DBTypeSource, IPServerSource, DBPortSource, DBUserSource, DBPassSource,DBSource,ApplicationIntentSource,InitialCatalogSource,TableSourceA,TableSourceB,TableSourceC,TableSourceD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,FieldSelect,WhereCondition,GroupByCondition,HavingCondition,OrderByCondition)
                                    isSPExecute = False

                                if(dataTransfer['rc']) :
                                    #update date param
                                    DBDestination = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddDestination, m.group()), DBDestinationFormat)
                                    TableDestination = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddDestination, m.group()), TableDestinationFormat)
                                    headerRaw = dataTransfer['headerRaw']
                                    dataInsert = dataTransfer['result']
                                    insertData = insertDestination(DBTypeDestination, IPServerDestination, DBPortDestination, DBUserDestination, DBPassDestination,DBDestination,ApplicationIntentDestination,InitialCatalogDestination, TableDestination, headerRaw, dataInsert,isSPExecute,FieldSelect)
                                    if insertData['rc']:
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< DataFlow :  Success untuk IDFitur "+str(IDFitur)+" Tanggal "+str(YYYYMMDD))
                                        updateLogActivity(str(IPServerIPLogActivity), str(DBUserIPLogActivity),str(DBPassIPLogActivity), str(ApplicationIntentLogActivity),str(InitialCatalogLogActivity), str(No), str('1'), str(dataTransfer['count']))
                                        createLog(IDFitur, str("Data Flow Sukses insert tanggal ")+ str(YYYYMMDD))
                                    else :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< DataFlow : Gagal insert data ke tabel destination tanggal "+ str(YYYYMMDD) +" untuk IDFitur "+ str(IDFitur))
                                        createLog(IDFitur, str("Gagal insert data tanggal ")+ str(YYYYMMDD) + str(' : ') + str(insertData['result']))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< DataFlow : DataSource Gagal ditarik "+str(dataTransfer['msg']))

                            else :
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Failed : Kriteria LogActivity tanggal "+ str(YYYYMMDD) +" belum terpenuhi untuk IDFitur "+ str(IDFitur))
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" : tidak ada Tanggal yang perlu diproses untuk IDFitur "+ str(IDFitur))
                else :
                    paramServerr = ''
                    if(not FiturIPSource['rc']) :
                        paramServerr= paramServerr + ' IPSource'
                    if(not FiturIPDestination['rc']) :
                        paramServerr= paramServerr + ' IPDestination'
                    if(not FiturIPLogActivity['rc']) :
                        paramServerr= paramServerr + ' IPLogActivity'
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" : Parameter koneksi untuk server"+str(paramServerr)+" Fitur "+ str(IDFitur )+" tidak ditemukan")
                    createLog(IDFitur, str('Parameter koneksi tidak ditemukan'))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" : Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" tidak ditemukan Fitur yang terdaftar")
        createLog(0, str("tidak ditemukan Fitur yang terdaftar"))


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