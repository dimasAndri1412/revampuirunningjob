import csv
import Config
import sys
from datetime import timedelta, date, datetime
import pyodbc
import ConnectDB
import threading
import os
import re
import traceback
import pandas as pd
from DatasourceDistributorConf import getFiturCon, insertDestination
from LogActivity import getLogActivity, updateLogActivity, cekLogActivity
from Etc import sanitize, passDecrypt

class DistThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter=counter
    def run(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        parse(self.counter)
        threadLock.release()

def parse(counter):
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0 :
        for dataFitur in Fiturs :
            try :
                ID, IDFitur, LocalPathFormat, FilenameFormat, TableDestination, FieldFilename, FieldFiledate, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityFilenameDateAdd, IsFixedCol, Delimiter, HeaderRowSkip, FooterRowSkip, TextQualifier = dataFitur
                #Get Fitur Connection
                FiturCon = getFiturCon(IDFitur)
                if FiturCon['rc'] :
                    IPServer, DBName, DBUser, DBPass, DBType = FiturCon['result']
                    #Get LogActivity
                    Tanggals = getLogActivity(IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)
                    if Tanggals['rc'] :
                        for dataTanggal in Tanggals['result'] :
                            No = dataTanggal[0]
                            Filedate = dataTanggal[1].strftime("%Y%m%d")

                            # Get LogActivity Criteria Run
                            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
                            CekLogActivityTrue = cekCriteria(ID, Filedate, IPServer)

                            if CekLogActivityTrue :
                                try:
                                    ConFitur = ConnectDB.connect_DB(IPServer, DBName, DBUser, DBPass)
                                    CurFitur = ConFitur.cursor()
                                    #Update StartDate
                                    updateLogActivity(str(IPServer), str(No), str(0))

                                    Filename = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(Filedate, LogActivityFilenameDateAdd, m.group()), FilenameFormat)
                                    LocalPath = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(Filedate, LogActivityFilenameDateAdd, m.group()), LocalPathFormat)

                                    if str(IsFixedCol) == '0' :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") +" Parse file with delimiter")
                                        StatusParse = parseDelimited(DBType, IPServer, DBName, DBUser, DBPass, ID, Filename, Filedate, LocalPath, TableDestination, FieldFilename, FieldFiledate, Delimiter, HeaderRowSkip, FooterRowSkip, TextQualifier)
                                    else :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Parse file with fixed column")
                                        StatusParse = parseFixedWidth(DBType, IPServer, DBName, DBUser, DBPass, ID, Filename, Filedate, LocalPath, TableDestination, FieldFilename, FieldFiledate, HeaderRowSkip, FooterRowSkip, TextQualifier)

                                    if StatusParse['rc'] :
                                        updateLogActivity(str(IPServer), str(No), str('1'), str(StatusParse['result']))
                                    else :
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Gagal parsing file tanggal "+ str(Filedate) +" untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                                except Exception as e:
                                    message = traceback.format_exc()
                                    print(message)
                                    ConFitur.rollback()
                                finally:
                                    CurFitur.close()
                            else :
                                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" Kriteria LogActivity belum terpenuhi")
                    else :
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << tidak ada Tanggal yang perlu diproses untuk Param "+ str(ID) +" Fitur "+ str(IDFitur))
                else :
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Parameter koneksi untuk Param "+ str(ID) +" Fitur "+ str(IDFitur )+" tidak ditemukan")
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Selesai memproses Fitur yang terdaftar")
    else :
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << tidak ditemukan Fitur yang terdaftar")

def getFitur(counter):
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select 
							ID, IDFitur, LocalPath, Filename, TableDestination, FieldFilename, FieldFiledate, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityFilenameDateAdd, IsFixedCol, Delimiter, HeaderRowSkip, FooterRowSkip, TextQualifier 
						FROM dbo.ParamParseFile with(nolock) 
						where IDFitur = 1 and ID in (1365) and isActive=1 and IDFitur in (9501117)""")

        curDataSourceDistributor.execute(qryGetFitur.format(TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataSourceDistributor.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

def cekCriteria(IDParamParseFile, YYYYMMDD, IPServer):
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        qryFiturCekLogActivity = ("""select 
										LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDateAdd 
									from dbo.ParamParseFileCekLogActivity with(nolock) 
									where IDParamParseFile={IDParamParseFile} and isActive=1 """)
        #print(qryFiturCekLogActivity.format(IDParamParseFile=sanitize(IDParamParseFile)))
        curDataSourceDistributor.execute(qryFiturCekLogActivity.format(IDParamParseFile=int(sanitize(IDParamParseFile))))
        FiturCekLogActivity = curDataSourceDistributor.fetchall()
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
        message = traceback.format_exc()
        print(message)
        return False
    finally :
        curDataSourceDistributor.close()

def replaceDateParam(DataDate, DateAdd, stringtoreplace):
    formatTgl = re.sub('[\{\}]', '', stringtoreplace)
    toDataDate = datetime.strptime(DataDate, "%Y%m%d")
    Tanggal = toDataDate + timedelta(days=DateAdd)
    return Tanggal.strftime(formatTgl)

def parseDelimited(DBType, IPServer, DBName, DBUser, DBPass, IDParam, Filename, Filedate, LocalPath, TableDestination, FieldFilename, FieldFiledate, Delimiter, HeaderRowSkip, FooterRowSkip, TextQualifier):
    try :
        columnMapping = getColumnMapping(IDParam)
        columnName = []
        usecol = []
        for x in columnMapping:
            columnName.append(x[1])
            usecol.append(x[0]-1)

        path = (LocalPath+Filename)

        num_lines = 0
        with open(path, 'r') as f:
            for line in f:
                num_lines += 1

        #print("lines : "+str(num_lines))
        if num_lines > HeaderRowSkip+FooterRowSkip :
            if TextQualifier == "" :
                df = pd.read_csv(path, delimiter=Delimiter, header=None, skiprows=HeaderRowSkip, skipfooter=FooterRowSkip, dtype=str, engine='python', usecols=usecol, encoding='unicode_escape')
            else :
                df = pd.read_csv(path, delimiter=Delimiter, quotechar=TextQualifier, header=None, skiprows=HeaderRowSkip, skipfooter=FooterRowSkip, dtype=str, engine='python', usecols=usecol, encoding='unicode_escape')

            #filling missing value using fillna()
            df.fillna(0, inplace=True)
            df.columns = columnName
            df = df.assign(**{FieldFilename:Filename})
            df = df.assign(**{FieldFiledate:Filedate})
            columnName.extend([FieldFilename, FieldFiledate])
            #print('Contents of the Dataframe from csv file ')
            #print(df)

            insert = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableDestination, columnName, df)
            return insert
        else :
            return {'rc':True, 'result': str(0)}
    except Exception as e:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> "+str(e))
        print(traceback.format_exc())
        return False

def parseFixedWidth(DBType, IPServer, DBName, DBUser, DBPass, IDParam, Filename, Filedate, LocalPath, TableDestination, FieldFilename, FieldFiledate, HeaderRowSkip, FooterRowSkip, TextQualifier):
    try :
        columnMapping = getColumnMapping(IDParam)
        columnName = []
        for x in columnMapping:
            columnName.append(x[1])

        path = (LocalPath+Filename)
        fixedColSubstring = getfixedColSubstring(IDParam)
        col_specification = []
        for i in range(len(fixedColSubstring)):
            x = []
            x.append(int(fixedColSubstring[i][0]))
            x.append(int(fixedColSubstring[i][1]))
            col_specification.append(x)

        num_lines = 0
        with open(path, 'r') as f:
            for line in f:
                num_lines += 1

        if num_lines > HeaderRowSkip+FooterRowSkip :
            print(col_specification)
            df = pd.read_fwf(path, colspecs=col_specification, header=None, skiprows=HeaderRowSkip, skipfooter=FooterRowSkip, dtype=str, encoding = 'unicode_escape')
            # filling missing value using fillna()
            df.fillna(0, inplace=True)
            df.columns = columnName
            df = df.assign(**{FieldFilename:Filename})
            df = df.assign(**{FieldFiledate:Filedate})
            columnName.extend([FieldFilename, FieldFiledate])
            print(df)

            insert = insertDestination(DBType, IPServer, DBName, DBUser, DBPass, TableDestination, columnName, df)
            return insert
        else :
            return {'rc':True, 'result': str(0)}
    except Exception as e:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> "+str(e))
        print(traceback.format_exc())
        return False

def getfixedColSubstring(IDParam):
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        qryGetFixedColSubstring = ("""select 
										dbo.fnSplitString(',', FixedColSubstring, 1) FixedColSubstringStart, 
										dbo.fnSplitString(',', FixedColSubstring, 2) FixedColSubstringEnd  
									from ParamParseFileMappingColumn with(nolock)
									where IDParamParseFile = {IDParam} 
									order by Col""")

        curDataSourceDistributor.execute(qryGetFixedColSubstring.format(IDParam=int(sanitize(IDParam))))
        FixedColSubstring = curDataSourceDistributor.fetchall()
        return FixedColSubstring
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

def getColumnMapping(IDParam) :
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        qryGetColumnMapping = ("""select 
									Col, FieldTargetName, FixedColSubstring 
								FROM dbo.ParamParseFileMappingColumn with(nolock) 
								where IDParamParseFile = {IDParam} 
								order by Col""")

        curDataSourceDistributor.execute(qryGetColumnMapping.format(IDParam=int(sanitize(IDParam))))
        ColumnMapping = curDataSourceDistributor.fetchall()
        return ColumnMapping
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally :
        curDataSourceDistributor.close()

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