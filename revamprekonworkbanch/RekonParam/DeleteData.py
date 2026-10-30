import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import threading
import os
import traceback
import re

from DatasourceDistributor.ParseReport import replaceDateParam
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
        DeleteData(self.counter)
        threadLock.release()


def qryDeleteData(conn, TableNameA,TableNameB,TableNameC,TableNameD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,WhereCondition):
    try:
        curr = conn.cursor()
        #list table
        #joinkey
        TableSourceName = TableNameA
        if(len(TableNameB)>0) :
            TableSourceName = TableSourceName +" "+ AJoinBType +" "+ TableNameB + " ON " + AJoinBKey
        if(len(TableNameC)>0) :
            TableSourceName = TableSourceName +" "+ ABJoinCType +" "+ TableNameC + " ON " + ABJoinCKey
        if(len(TableNameD)>0) :
            TableSourceName = TableSourceName +" "+ ABCJoinDType +" "+ TableNameD + " ON " + ABCJoinDKey



        #where
        WhereConditionName = ""
        if(len(WhereCondition)>0) :
            WhereConditionName = "WHERE "+WhereCondition



        qry = ("""DELETE {TableNameA} FROM {TableSourceName} {WhereConditionName}""")


        if Config.PrintQry['qryDelete'] :
            print("===============Begin Of qryDeleteData===============")
            print(qry.format(TableNameA=sanitize(TableNameA),TableSourceName=sanitize(TableSourceName),WhereConditionName=sanitize(WhereConditionName)))
            print("===============End Of qryDeleteData===============")
        curr.execute(qry.format(TableNameA=sanitize(TableNameA),TableSourceName=sanitize(TableSourceName),WhereConditionName=sanitize(WhereConditionName)))
        return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        #conn.rollback()
        return {'rc' : False, 'msg' : str(message) +' : query => '+ str(qry.format(TableNameA=sanitize(TableNameA),TableSourceName=sanitize(TableSourceName),WhereConditionName=sanitize(WhereConditionName)))}

def getFitur(counter):
    try:
        ConRekonParam = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        CurRekonParam = ConRekonParam.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select 
                                IDFitur, Name, a.IPServer, a.DBName, DBUser, DBPass, LogActivityModul, LogActivitySubModul, LogActivityEntity 
                            from ParamFitur a inner join (
                                select row_number() over (order by IPServer, DBName) RN, IPServer, DBName
                                From (select distinct IPServer, DBName FROM ParamFitur with(nolock)) as RN
                            ) as b
                        on a.IPServer=b.IPServer
                            and a.DBName=b.DBName
                        where isActive=2 and IDFitur IN (9502202,9502203,9502237,9502238) and b.RN% {TotalThread} = {counter} order by IDFitur""")
        if Config.PrintQry['getFitur'] :
            print("===============Begin Of getFitur===============")
            print(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
            print("===============End Of getFitur===============")
        CurRekonParam.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = CurRekonParam.fetchall()
        return {'rc':True, 'result':Fiturs}
    except Exception as e:
        return {'rc':False, 'result':[]}
    finally :
        CurRekonParam.close()

def cekCriteria(IDFitur, YYYYMMDD, IPServer):
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

                toDataDate = datetime.strptime(YYYYMMDD, "%Y%m%d")
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
        return False
        message = traceback.format_exc()
        print(message)
    finally :
        Cur.close()

def getParamFitur(IDFitur):
    try:
        ConRekonParam = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        CurRekonParam = ConRekonParam.cursor()

        qryGetParamJoin = ("""Select 
                                ID,FiturName,TableSourceA,TableSourceB,TableSourceC,TableSourceD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,WhereCondition,LogActivityDateAdd
                            From ParamDelete with(nolock) where IDFItur={IDFitur} and isActive=1 Order by OrderExecute""")
        if Config.PrintQry['getParamFitur'] :
            print("===============Begin Of getParamFitur===============")
            print(qryGetParamJoin.format(IDFitur=int(sanitize(IDFitur))))
            print("===============End Of getParamFitur===============")
        CurRekonParam.execute(qryGetParamJoin.format(IDFitur=int(sanitize(IDFitur))))
        ParamJoin = CurRekonParam.fetchall()
        return {'rc':True, 'result':ParamJoin}
    except Exception as e:
        return {'rc':False, 'result':[]}
    finally :
        CurRekonParam.close()

def DeleteData(counter):
    # Get Fitur
    Fiturs = getFitur(counter)
    if Fiturs['rc']:
        for dataFitur in Fiturs['result'] :
            IDFitur, Name, IPServer, DBName, DBUser, DBPass, LogActivityModul, LogActivitySubModul, LogActivityEntity = dataFitur
            #GetFitur
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil param delete yang terdaftar")
            GetParamDelete = getParamFitur(IDFitur)
            # Get LogActivity
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Tanggal Rekon yang perlu diproses untuk Fitur "+ str(IDFitur))
            Tanggals = getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)
            if Tanggals['rc'] :
                for dataTanggal in Tanggals['result'] :
                    No = dataTanggal[0]
                    YYYYMMDD = dataTanggal[1].strftime("%Y%m%d")
                    DataDateString = dataTanggal[2]

                    # Get LogActivity Criteria Run
                    CekLogActivityTrue = cekCriteria(IDFitur, YYYYMMDD, IPServer)

                    if CekLogActivityTrue :
                        try:
                            ConFitur = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+passDecrypt(str(DBPass), str(IPServer)+Config.saltKeyadd))
                            CurFitur = ConFitur.cursor()
                            continueExec = True
                            updateLogActivity(str(IPServer), str(No), str(0))

                            doneDeleteData=True
                            if GetParamDelete['rc'] :
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" DataDate-"+YYYYMMDD+" : Begin Transaction Delete ")
                                for ParamDelete in GetParamDelete['result'] :
                                    ID,FiturName,TableSourceAFormat,TableSourceBFormat,TableSourceCFormat,TableSourceDFormat,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKeyFormat,ABJoinCKeyFormat,ABCJoinDKeyFormat,WhereConditionFormat,LogActivityDateAddParam = ParamDelete
                                    TableSourceA = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), TableSourceAFormat)
                                    TableSourceB = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), TableSourceBFormat)
                                    TableSourceC = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), TableSourceCFormat)
                                    TableSourceD = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), TableSourceDFormat)



                                    AJoinBKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), AJoinBKeyFormat)
                                    ABJoinCKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), ABJoinCKeyFormat)
                                    ABCJoinDKey = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), ABCJoinDKeyFormat)


                                    WhereCondition = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(YYYYMMDD, LogActivityDateAddParam, m.group()), WhereConditionFormat)

                                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Deleting Data . . . ")

                                    doneDeleteData = qryDeleteData(ConFitur, TableSourceA,TableSourceB,TableSourceC,TableSourceD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,WhereCondition)
                                    if doneDeleteData['rc'] :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Data Deleted")
                                        createLog(IDFitur, ID, str("Done delete data FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD))
                                    else :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Data Not Deleted")
                                        continueExec=False
                                        createLog(IDFitur, ID, str("Gagal delete data FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" "+doneDeleteData['msg']))
                                        break
                            else :
                                createLog(IDFitur, 0, str("Tidak terdapat param delete pada FiturID-"+str(IDFitur)))
                                break

                            if continueExec :
                                ConFitur.commit()
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" DataDate-"+YYYYMMDD+" : Commit Transaction Delete ")
                                updateLogActivity(str(IPServer), str(No), str(1))
                            else :
                                CurFitur.close()
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" DataDate-"+YYYYMMDD+" : Rollback Transaction Delete ")
                                createLog(IDFitur, str('0'), str('Rollback task IDFitur: ')+str(IDFitur)+str(' tanggal ')+str(YYYYMMDD))
                        except Exception as e:
                            message = traceback.format_exc()
                            print(message)
                            ConFitur.rollback()
                        finally:
                            ConFitur.close()
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" Kriteria LogActivity belum terpenuhi")

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