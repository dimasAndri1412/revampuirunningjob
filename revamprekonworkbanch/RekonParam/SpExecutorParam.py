import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import threading
import os
import traceback
import re
#from RekonParamConf import getFiturCon #, insertDestination, createLog
from LogActivity import getLogActivity, updateLogActivity, cekLogActivity
from Etc import row_gen, checkColumnExist, addAliasTable, joinQry, createLog, sanitize, passDecrypt


class DistThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter=counter
    def run(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        ExecuteSP(self.counter)
        threadLock.release()


def replaceDateParam(DataDate, DateAdd, stringtoreplace):
    formatTgl = re.sub('[\{\}]', '', stringtoreplace)
    toDataDate = datetime.strptime(DataDate, "%Y%m%d")
    Tanggal = toDataDate + timedelta(days=DateAdd)
    return Tanggal.strftime(formatTgl)

def getFitur(counter):
    try:
        ConRekonParam = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        CurRekonParam = ConRekonParam.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select 
							IDFitur, Name, IPServer, DBName, DBUser, DBPass, LogActivityModul, LogActivitySubModul, LogActivityEntity, isActive
						from dbo.ParamFitur  with(nolock) 
						where isActive=3 AND IDFitur in (4631) and IDFitur%{TotalThread} = {counter}""")
        if Config.PrintQry['getFitur'] :
            print("===============Begin Of getFitur===============")
            print(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
            print("===============End Of getFitur===============")
        CurRekonParam.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = CurRekonParam.fetchall()
        #print(Fiturs)
        return {'rc':True, 'result':Fiturs}
    except Exception as e:
        return {'rc':False, 'result':[]}
    finally :
        CurRekonParam.close()

def getSPExecutor(IDFitur):
    try:

        ConSpExecutor = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        CurSpExecutor = ConSpExecutor.cursor()


        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Get SpExecutor yang terdaftar dari IDFitur = "+str(IDFitur))

        qrySpExecutor = ("""select 
							ID, IDFitur, OrderExecute, StoredProcedureName, LogActivityDateAdd, IsActive, Note
						from dbo.ParamSPExecutor  with(nolock) 
						where isActive=1 and IDFItur={IDFitur} ORDER BY OrderExecute ASC""")


        CurSpExecutor.execute(qrySpExecutor.format(IDFitur=int(sanitize(IDFitur))))
        SP = CurSpExecutor.fetchall()

        return {'rc':True, 'result':SP}

    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc':False, 'result':[]}




def cekCriteria(IDFitur, DataDate, IPServer):
    try:
        Con = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        Cur = Con.cursor()

        qryFiturCekLogActivity = ("""select 
                                        LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDateAdd 
                                    from dbo.ParamFiturCekLogActivity with(nolock) 
                                    where IDFitur={IDFitur} and isActive=1 """)
        if Config.PrintQry['cekCriteria'] :
            print("===============Begin Of cekCriteria===============")
            print(qryFiturCekLogActivity.format(IDFitur=sanitize(IDFitur)))
            print("===============End Of cekCriteria===============")
        Cur.execute(qryFiturCekLogActivity.format(IDFitur=sanitize(IDFitur)))
        FiturCekLogActivity = Cur.fetchall()
        CekLogActivityTrue = False
        if len(FiturCekLogActivity) > 0 :
            for CekLogActivity in FiturCekLogActivity :
                cekLogActivityModul, cekLogActivitySubModul, cekLogActivityEntity, LogActivityDateAdd = CekLogActivity

                toDataDate = datetime.strptime(DataDate, "%Y%m%d")
                Tanggal = toDataDate + timedelta(days=LogActivityDateAdd)

                cekDone = cekLogActivity(IPServer, cekLogActivityModul, cekLogActivitySubModul, cekLogActivityEntity, Tanggal.strftime("%Y%m%d"))

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
            CekLogActivityTrue = True
        return CekLogActivityTrue
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return False

    finally :
        Cur.close()


def InititalizeSP(StoredProcedureName, LogActivityDateAdd ,YYYYMMDD):
    try :
        if StoredProcedureName==0:
            None
        else :
            #print(YYYYMMDD)
            StoredProcedureName = re.sub(r'\{[\%a-zA-Z0-9]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAdd, m.group()), StoredProcedureName)
            #print(sanitize(StoredProcedureName))
            qrySelect = """EXEC {StoredProcedureName} """
            return {'rc':True, 'result':qrySelect.format(StoredProcedureName=sanitize(StoredProcedureName))}

    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc':False}
    #finally :
    # cur.close()



def ExecuteSP(counter):

    Fiturs = getFitur(counter)

    if Fiturs['rc']:
        for dataFitur in Fiturs['result'] :
            try :
                ID = None
                IDFitur = dataFitur[0]

                IPServer = dataFitur[2]
                DBName = dataFitur[3]
                DBUser= dataFitur[4]
                DBPass = dataFitur[5]
                LogActivityModul = dataFitur[6]
                LogActivitySubModul = dataFitur[7]
                LogActivityEntity = dataFitur[8]

                #Get LogActivity
                Tanggals = getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)

                if Tanggals['rc'] :
                    for dataTanggal in Tanggals['result'] :
                        No = dataTanggal[0]
                        DataDate = dataTanggal[1]
                        YYYYMMDD = dataTanggal[1].strftime("%Y%m%d")

                        #Update StartDate
                        updateLogActivity(str(IPServer), str(No), str(0))
                        #CekLogActivity
                        #CekLogActivityTrue = cekLogActivity(str(IPServer),LogActivityModul,LogActivitySubModul,LogActivityEntity,YYYYMMDD)
                        CekLogActivityTrue =  cekCriteria(IDFitur, YYYYMMDD, IPServer)
                        if CekLogActivityTrue :
                            ListSPExecutor = getSPExecutor(IDFitur)

                            if ListSPExecutor['rc']:
                                isSuccess = False

                                for SPExecutor in ListSPExecutor['result']:
                                    #print(SPExecutor)

                                    ID = SPExecutor[0]
                                    OrderExecute = SPExecutor[2]
                                    StoredProcedureName = SPExecutor[3]
                                    LogActivityDateAdd = SPExecutor[4]

                                    Init = InititalizeSP(StoredProcedureName,LogActivityDateAdd, YYYYMMDD)


                                    con = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
                                    cur = con.cursor()
                                    con.autocommit = True
                                    cur.execute(Init['result'])

                                    isSuccess = True

                                if isSuccess == True :
                                    updateLogActivity(str(IPServer), str(No), str(1))
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Sukses Menjalankan SP Executor pada tanggal " + str(YYYYMMDD))
                                else :
                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Gagal Menjalankan SP Executor pada tanggal " + str(YYYYMMDD))



                            else:
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << ParamSPExecutor Tidak Ditemukan untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                        else:
                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Failed : Kriteria LogActivity tanggal "+ str(DataDate) +" belum terpenuhi untuk IDFitur "+ str(IDFitur))


                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Selesai memproses semua Tanggal untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << tidak ada Tanggal yang perlu diproses untuk Param"+" Fitur "+ str(IDFitur))
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << tidak ditemukan Fitur yang terdaftar")

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