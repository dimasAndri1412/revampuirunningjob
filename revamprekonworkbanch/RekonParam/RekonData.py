#!/usr/bin/python
import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import threading
import os
import traceback
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
        Rekon(self.counter)
        threadLock.release()


def qrySelect(SourceTable, SourceDataDateCol, DataDateString, SourceWhere, SourcePartitionCol, SourceOrderCol):
    try:
        if len(SourceWhere)==0 :
            SourceWhere="1=1"
        # Set Variable Where Clause source1
        if len(SourceDataDateCol)>0 :
            qryWhereSource = " and ".join(["1=1", SourceWhere, SourceDataDateCol+"='"+DataDateString+"'"])
        else :
            qryWhereSource = " and ".join(["1=1", SourceWhere])

        # Set Variable Row Number
        if len(SourcePartitionCol)>0 and len(SourceOrderCol)>0 :
            qryPartition = " Row_number() over(partition by "+str(SourcePartitionCol)+" order by "+str(SourceOrderCol)+") as [RowNumber] "
        elif len(SourcePartitionCol)==0 and len(SourceOrderCol)>0 :
            qryPartition = " Row_number() over(order by "+str(SourceOrderCol)+") as [RowNumber] "
        else :
            qryPartition = " Convert(int, 1) [RowNumber] "

        # Query Select Source1
        qrySelectSource = ("""Select {qryPartition}, * 
                            From {SourceTable} With(nolock) 
                            Where {qryWhereSource} """)
        if Config.PrintQry['qrySelect'] :
            print("===============Begin Of qrySelect===============")
            print(qrySelectSource.format(qryPartition=sanitize(qryPartition), SourceTable=sanitize(SourceTable), qryWhereSource=sanitize(qryWhereSource)))
            print("===============End Of qrySelect===============")
        return qrySelectSource.format(qryPartition=sanitize(qryPartition), SourceTable=sanitize(SourceTable), qryWhereSource=sanitize(qryWhereSource))
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return str("")

def qryUpdateA(conn, Source, ColUpdate, ColSelect):
    try:
        curr = conn.cursor()
        qryUpd = joinQry(",", "A", ColUpdate, "A", ColSelect)
        qry = ("""Update A Set {qryUpd} From ({Source}) A """)
        #print("Update A Set "+ qryUpd + "From" + Source +" A")
        if Config.PrintQry['qryUpdateA'] :
            print("===============Begin Of qryUpdateA===============")
            print(qry.format(qryUpd=sanitize(qryUpd), Source=sanitize(Source)))
            print("===============End Of qryUpdateA===============")
        curr.execute(qry.format(qryUpd=sanitize(qryUpd), Source=sanitize(Source)))
        #conn.commit()
        return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        #print(qry.format(qryUpd=sanitize(qryUpd), Source=sanitize(Source)))
        #conn.rollback()
        return {'rc' : False, 'msg' : str(message) +' : query => '+ str(qry.format(qryUpd=sanitize(qryUpd), Source=sanitize(Source)))}

def qryUpdateAfromB(conn, SourceA, SourceB, SourceAJoin, SourceBJoin, SourceAUpdate, SourceBUpdate):
    try:
        curr = conn.cursor()
        qryUpdate = joinQry(",", "A", SourceAUpdate, "B", SourceBUpdate)
        qryJoin = joinQry("and", "A", SourceAJoin, "B", SourceBJoin)
        qry = ("""Update A Set {qryUpdate}
                From ({SourceA}) A inner join ({SourceB}) B
                ON A.[RowNumber]=B.[RowNumber] 
                    and {qryJoin}""")
        #print("Update A Set "+ qryUpdate + "From (" + SourceA +" ) A" + " inner join (" + SourceB + " ) B AND" + qryJoin)
        if Config.PrintQry['qryUpdateAfromB'] :
            print("===============Begin Of qryUpdateAfromB===============")
            print(qry.format(qryUpdate=sanitize(qryUpdate), SourceA=sanitize(SourceA), SourceB=sanitize(SourceB), qryJoin=sanitize(qryJoin)))
            print("===============End Of qryUpdateAfromB===============")
        curr.execute(qry.format(qryUpdate=sanitize(qryUpdate), SourceA=sanitize(SourceA), SourceB=sanitize(SourceB), qryJoin=sanitize(qryJoin)))
        #print(qry.format(qryUpdate=sanitize(qryUpdate), SourceA=sanitize(SourceA), SourceB=sanitize(SourceB), qryJoin=sanitize(qryJoin)))
        #conn.commit()
        return {'rc' : True}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        #conn.rollback()
        return {'rc' : False, 'msg' : str(message) +' : query => '+ str(qry.format(qryUpdate=sanitize(qryUpdate), SourceA=sanitize(SourceA), SourceB=sanitize(SourceB), qryJoin=sanitize(qryJoin)))}

def qryMove(conn, SourceTable, SourceWhere, FlagMatchCol, FieldSelect, MoveTargetTable, SourceDataDateCol, DataDateString):
    try:
        cur = conn.cursor()
        if len(SourceDataDateCol)>0:
            DateWhere = SourceDataDateCol+"='"+DataDateString+"'"
        else :
            DateWhere = "1=1"
        if len(SourceWhere)==0 :
            SourceWhere = "1=1"
        if len(SourceDataDateCol)>0 :
            qryWhereSource = " and ".join(["1=1", SourceWhere, DateWhere])
        else :
            qryWhereSource = " and ".join(["1=1", SourceWhere])

        qry = ("""Select {FieldSelect} 
                From {SourceTable} With(Nolock) 
                Where {qryWhereSource} and {FlagMatchCol} Is NOt Null""")
        if Config.PrintQry['qryMove'] :
            print("===============Begin Of qryMove===============")
            print(qry.format(FieldSelect=sanitize(FieldSelect), SourceTable=sanitize(SourceTable), qryWhereSource=sanitize(qryWhereSource), FlagMatchCol=(FlagMatchCol)))
            print("===============End Of qryMove===============")
        cur.execute(qry.format(FieldSelect=sanitize(FieldSelect), SourceTable=sanitize(SourceTable), qryWhereSource=sanitize(qryWhereSource), FlagMatchCol=(FlagMatchCol)))
        if len(cur.description)>0 :
            colNameList = []
            colNames = str('')
            for i in range(len(cur.description)):
                desc = cur.description[i]
                colNameList.append(str('[')+desc[0]+str(']'))
            ColumnExist = checkColumnExist(cur, MoveTargetTable, colNameList)
            if ColumnExist :
                colNames = ','.join(colNameList)
                qrySelect = qry.format(FieldSelect=sanitize(FieldSelect), SourceTable=sanitize(SourceTable), qryWhereSource=sanitize(qryWhereSource), FlagMatchCol=(FlagMatchCol))

                qryInsert = ("Insert into {MoveTargetTable} ({colNames}) {qrySelect}")
                cur.execute(qryInsert.format(MoveTargetTable=sanitize(MoveTargetTable), colNames=sanitize(colNames), qrySelect=sanitize(qrySelect)))
                #conn.commit()
                qryDelete = ("Delete From {SourceTable} Where {SourceWhere} and {FlagMatchCol} Is NOt Null")
                cur.execute(qryDelete.format(SourceTable=sanitize(SourceTable), SourceWhere=sanitize(SourceWhere), FlagMatchCol=sanitize(FlagMatchCol)))
                #conn.commit()
            return {'rc' : True}
        else :
            return {'rc' : False, 'msg' : str('Gagal mendapatkan data untuk dipindahkan : query => ')+ str(qry)}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        #conn.rollback()
        return {'rc' : False, 'msg' : str('Gagal memindahkan data : ')+str(message)}

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
                        where isActive=1 and b.RN% {TotalThread} = {counter} and a.IDFitur in (9502225,9502226) order by IDFitur  """)
        #452,457,458,462,456,454,460,459
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
                                ID, Source1Table, Source2Table, Source1Where, Source2Where, 
                                Source1DataDateCol, Source2DataDateCol, 
                                Source1KeyJoinCol1, Source2KeyJoinCol1, Source1KeyJoinCol2, Source2KeyJoinCol2, Source1KeyJoinCol3, Source2KeyJoinCol3, Source1KeyJoinCol4, Source2KeyJoinCol4, Source1KeyJoinCol5, Source2KeyJoinCol5, Source1KeyJoinCol6, Source2KeyJoinCol6, Source1KeyJoinCol7, Source2KeyJoinCol7, Source1KeyJoinCol8, Source2KeyJoinCol8, Source1KeyJoinCol9, Source2KeyJoinCol9, Source1KeyJoinCol10, Source2KeyJoinCol10, 
                                Source1PartitionCol, Source2PartitionCol, Source1OrderCol, Source2OrderCol, 
                                Source1UpdateCol1, Source2UpdateData1, Source1UpdateCol2, Source2UpdateData2, Source1UpdateCol3, Source2UpdateData3, Source1UpdateCol4, Source2UpdateData4, Source1UpdateCol5, Source2UpdateData5, Source1UpdateCol6, Source2UpdateData6, Source1UpdateCol7, Source2UpdateData7, Source1UpdateCol8, Source2UpdateData8, Source1UpdateCol9, Source2UpdateData9, Source1UpdateCol10, Source2UpdateData10, 
                                Source2UpdateCol1, Source1UpdateData1, Source2UpdateCol2, Source1UpdateData2, Source2UpdateCol3, Source1UpdateData3, Source2UpdateCol4, Source1UpdateData4, Source2UpdateCol5, Source1UpdateData5, Source2UpdateCol6, Source1UpdateData6, Source2UpdateCol7, Source1UpdateData7, Source2UpdateCol8, Source1UpdateData8, Source2UpdateCol9, Source1UpdateData9, Source2UpdateCol10, Source1UpdateData10, 
                                Source1isMoveDataMatch, Source2isMoveDataMatch, Source1UniqueCol, Source2UniqueCol, 
                                Source1FlagMatchCol, Source2FlagMatchCol, Source1FieldSelectMove, Source2FieldSelectMove, Source1MoveTargetTable, Source2MoveTargetTable, isActive 
                            From ParamJoin with(nolock) where IDFItur={IDFitur} and isActive=1 Order by OrderExecute""")
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

def Rekon(counter):
    # Get Fitur
    Fiturs = getFitur(counter)
    if Fiturs['rc']:
        for dataFitur in Fiturs['result'] :
            IDFitur, Name, IPServer, DBName, DBUser, DBPass, LogActivityModul, LogActivitySubModul, LogActivityEntity = dataFitur
            # Get LogActivity
            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Tanggal Rekon yang perlu diproses untuk Fitur "+ str(IDFitur))
            Tanggals = getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)
            if Tanggals['rc'] :
                for dataTanggal in Tanggals['result'] :
                    No = dataTanggal[0]
                    YYYYMMDD = dataTanggal[1].strftime("%Y%m%d")
                    DataDateString = dataTanggal[2]

                    # Get LogActivity Criteria Run
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
                    CekLogActivityTrue = cekCriteria(IDFitur, YYYYMMDD, IPServer)
                    if CekLogActivityTrue :
                        try:
                            ConFitur = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(IPServer)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+passDecrypt(str(DBPass), str(IPServer)+Config.saltKeyadd))
                            CurFitur = ConFitur.cursor()
                            continueExec = True
                            updateLogActivity(str(IPServer), str(No), str(0))

                            #Get ParamJoin
                            GetParamJoin = getParamFitur(IDFitur)
                            doneUpdateSource1=True
                            doneUpdateSource2=True
                            doneFlagMatchSource1=True
                            doneFlagMatchSource2=True
                            doneMoveSource1=True
                            doneMoveSource2=True

                            if GetParamJoin['rc'] :
                                for ParamJoin in GetParamJoin['result'] :
                                    Source1KeyJoinCol = [None]*10
                                    Source2KeyJoinCol = [None]*10
                                    Source1UpdateCol = [None]*10
                                    Source2UpdateData = [None]*10
                                    Source2UpdateCol = [None]*10
                                    Source1UpdateData = [None]*10
                                    Source1UniqueCol = [0]
                                    Source2UniqueCol = [0]
                                    Source1FlagMatchCol = [0]
                                    Source2FlagMatchCol =[0]
                                    # Set Variable from Param
                                    ID, Source1Table, Source2Table, Source1Where, Source2Where, Source1DataDateCol, Source2DataDateCol, Source1KeyJoinCol[0], Source2KeyJoinCol[0], Source1KeyJoinCol[1], Source2KeyJoinCol[1], Source1KeyJoinCol[2], Source2KeyJoinCol[2], Source1KeyJoinCol[3], Source2KeyJoinCol[3], Source1KeyJoinCol[4], Source2KeyJoinCol[4], Source1KeyJoinCol[5], Source2KeyJoinCol[5], Source1KeyJoinCol[6], Source2KeyJoinCol[6], Source1KeyJoinCol[7], Source2KeyJoinCol[7], Source1KeyJoinCol[8], Source2KeyJoinCol[8], Source1KeyJoinCol[9], Source2KeyJoinCol[9], Source1PartitionCol, Source2PartitionCol, Source1OrderCol, Source2OrderCol, Source1UpdateCol[0], Source2UpdateData[0], Source1UpdateCol[1], Source2UpdateData[1], Source1UpdateCol[2], Source2UpdateData[2], Source1UpdateCol[3], Source2UpdateData[3], Source1UpdateCol[4], Source2UpdateData[4], Source1UpdateCol[5], Source2UpdateData[5], Source1UpdateCol[6], Source2UpdateData[6], Source1UpdateCol[7], Source2UpdateData[7], Source1UpdateCol[8], Source2UpdateData[8], Source1UpdateCol[9], Source2UpdateData[9], Source2UpdateCol[0], Source1UpdateData[0], Source2UpdateCol[1], Source1UpdateData[1], Source2UpdateCol[2], Source1UpdateData[2], Source2UpdateCol[3], Source1UpdateData[3], Source2UpdateCol[4], Source1UpdateData[4], Source2UpdateCol[5], Source1UpdateData[5], Source2UpdateCol[6], Source1UpdateData[6], Source2UpdateCol[7], Source1UpdateData[7], Source2UpdateCol[8], Source1UpdateData[8], Source2UpdateCol[9], Source1UpdateData[9], Source1isMoveDataMatch, Source2isMoveDataMatch, Source1UniqueCol[0], Source2UniqueCol[0], Source1FlagMatchCol[0], Source2FlagMatchCol[0], Source1FieldSelectMove, Source2FieldSelectMove, Source1MoveTargetTable, Source2MoveTargetTable, isActive = ParamJoin

                                    # Source1
                                    if len(Source1Table)>0 :
                                        qrySelectSource1 = qrySelect(Source1Table, Source1DataDateCol, DataDateString, Source1Where, Source1PartitionCol, Source1OrderCol)
                                    else :
                                        qrySelectSource1 = str("")

                                    # Source2
                                    if len(Source2Table)>0 :
                                        qrySelectSource2 = qrySelect(Source2Table, Source2DataDateCol, DataDateString, Source2Where, Source2PartitionCol, Source2OrderCol)
                                    else :
                                        qrySelectSource2 = str("")

                                    #print(qrySelectSource2, qrySelectSource1, Source2KeyJoinCol, Source1KeyJoinCol, Source2FlagMatchCol, Source1UniqueCol)

                                    # Update from 1 Source Data
                                    if len(qrySelectSource1)>0 and len(qrySelectSource2)==0 :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Update Source1")
                                        doneUpdateSource1 = qryUpdateA(ConFitur, qrySelectSource1, Source1UpdateCol, Source2UpdateData)
                                        if doneUpdateSource1['rc'] :
                                            createLog(IDFitur, ID, str('Done update data dari 1 Source Data tanggal '+YYYYMMDD))
                                        else :
                                            continueExec=False
                                            createLog(IDFitur, ID, str('Gagal update data dari 1 Source Data tanggal '+YYYYMMDD+': ')+doneUpdateSource1['msg'])

                                    # Update from 2 Source Data
                                    if len(qrySelectSource1)>0 and len(qrySelectSource2)>0 :
                                        # Update Flag Match Source1
                                        if len(Source1FlagMatchCol[0])>0 and len(Source2UniqueCol[0]) > 0 :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" QryID-"+str(ID)+" DataDate-"+YYYYMMDD+" Update FlagMatch1 from Source1 Join Source2")
                                            doneFlagMatchSource1=qryUpdateAfromB(ConFitur, qrySelectSource1, qrySelectSource2, Source1KeyJoinCol, Source2KeyJoinCol, Source1FlagMatchCol, Source2UniqueCol)
                                            if doneFlagMatchSource1['rc'] :
                                                createLog(IDFitur, ID, str('Done Update FlagMatch1 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Update FlagMatch1 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneFlagMatchSource1['msg'])
                                        # Update Flag Match Source2
                                        if len(Source2FlagMatchCol[0])>0 and len(Source1UniqueCol[0]) > 0 :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" QryID-"+str(ID)+" DataDate-"+YYYYMMDD+" Update FlagMatch2 from Source1 Join Source2")
                                            doneFlagMatchSource2 = qryUpdateAfromB(ConFitur, qrySelectSource2, qrySelectSource1, Source2KeyJoinCol, Source1KeyJoinCol, Source2FlagMatchCol, Source1UniqueCol)
                                            if doneFlagMatchSource2['rc'] :
                                                createLog(IDFitur, ID, str('Done Update FlagMatch2 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Update FlagMatch2 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneFlagMatchSource2['msg'])
                                        # Update Source1
                                        if len(Source1UpdateCol[0])>0 and len(Source2UpdateData[0])>0 :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Update Source1 from Source1 Join Source2")
                                            doneUpdateSource1 = qryUpdateAfromB(ConFitur, qrySelectSource1, qrySelectSource2, Source1KeyJoinCol, Source2KeyJoinCol, Source1UpdateCol, Source2UpdateData)
                                            if doneUpdateSource1['rc'] :
                                                createLog(IDFitur, ID, str('Done Update Source1 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Update Source1 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneUpdateSource1['msg'])
                                        # Update Source2
                                        if len(Source2UpdateCol[0])>0 and len(Source1UpdateData[0])>0 :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" Qry-"+str(ID)+" DataDate-"+YYYYMMDD+" Update Source2 from Source1 Join Source2")
                                            doneUpdateSource2 = qryUpdateAfromB(ConFitur, qrySelectSource2, qrySelectSource1, Source2KeyJoinCol, Source1KeyJoinCol, Source2UpdateCol, Source1UpdateData)
                                            if doneUpdateSource2['rc'] :
                                                createLog(IDFitur, ID, str('Done Update Source2 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Update Source2 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneUpdateSource2['msg'])

                                        # Move Match Source1
                                        if len(Source1FlagMatchCol[0])>0 and len(Source2UniqueCol[0]) > 0 and str(Source1isMoveDataMatch)=="1" and str(Source1MoveTargetTable)!="" :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" QryID-"+str(ID)+" DataDate-"+YYYYMMDD+" Move Match1 from Source1 Join Source2")
                                            doneMoveSource1 = qryMove(ConFitur, Source1Table, Source1Where, Source1FlagMatchCol[0], Source1FieldSelectMove, Source1MoveTargetTable, Source1DataDateCol, DataDateString)
                                            if doneMoveSource1['rc'] :
                                                createLog(IDFitur, ID, str('Done Move Match1 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Move Match1 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneMoveSource1['msg'])
                                        # Move Match Source2
                                        if len(Source2FlagMatchCol[0])>0 and len(Source1UniqueCol[0]) > 0 and str(Source2isMoveDataMatch)=="1" and str(Source2MoveTargetTable)!="" :
                                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" FiturID-"+str(IDFitur)+" QryID-"+str(ID)+" DataDate-"+YYYYMMDD+" Move Match2 from Source1 Join Source2")
                                            doneMoveSource2 = qryMove(ConFitur, Source2Table, Source2Where, Source2FlagMatchCol[0], Source2FieldSelectMove, Source2MoveTargetTable, Source2DataDateCol, DataDateString)
                                            if doneMoveSource2['rc'] :
                                                createLog(IDFitur, ID, str('Done Move Match2 from Source1 Join Source2 tanggal '+YYYYMMDD))
                                            else :
                                                continueExec=False
                                                createLog(IDFitur, ID, str('Gagal Move Match2 from Source1 Join Source2 tanggal '+YYYYMMDD+': ')+doneMoveSource2['msg'])
                                    if continueExec==False :
                                        break
                            if continueExec :
                                ConFitur.commit()
                                updateLogActivity(str(IPServer), str(No), str(1))
                            else :
                                CurFitur.close()
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