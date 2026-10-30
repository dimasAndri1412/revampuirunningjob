from contextlib import AbstractAsyncContextManager
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
import shutil
import subprocess


class DistThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        zip(self.counter)
        threadLock.release()


def zip(counter):
    Fiturs = getFitur(counter)
    if len(Fiturs) > 0:
        for dataFitur in Fiturs:
            try:
                ID, IDFitur, Action, SourcePathFormat, FileNameFormat, Extension, Password, DestinationPathFormat, isMoveToBackUp, BackupPathFormat, IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityFilenameDateAdd = dataFitur
                # Get Fitur Connection
                FiturCon = getFiturCon(IDFitur)
                if FiturCon['rc']:
                    IPServer, DBName, DBUser, DBPass, DBType = FiturCon['result']
                    # Get LogActivity
                    Tanggals = getLogActivity(
                        IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity)
                    if Tanggals['rc']:
                        for dataTanggal in Tanggals['result']:
                            No = dataTanggal[0]
                            Filedate = dataTanggal[1].strftime("%Y%m%d")

                            # Get LogActivity Criteria Run
                            print(datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
                            CekLogActivityTrue = cekCriteria(
                                IDFitur, Filedate, IPServer)

                            if CekLogActivityTrue:
                                try:
                                    ConFitur = ConnectDB.connect_DB(IPServer, DBName, DBUser, DBPass)
                                    CurFitur = ConFitur.cursor()
                                    # Update StartDate
                                    updateLogActivity(
                                        str(IPServer), str(No), str(0))

                                    FileName = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(
                                        Filedate, LogActivityFilenameDateAdd, m.group()), FileNameFormat)
                                    SourcePath = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(
                                        Filedate, LogActivityFilenameDateAdd, m.group()), SourcePathFormat)
                                    DestinationPath = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(
                                        Filedate, LogActivityFilenameDateAdd, m.group()), DestinationPathFormat)
                                    BackupPath = re.sub(r'\{[\%a-zA-Z0-9\-\_\.]+\}', lambda m: replaceDateParam(
                                        Filedate, LogActivityFilenameDateAdd, m.group()), BackupPathFormat)

                                    if str(Action) == 'UNZIP':
                                        print(datetime.today().strftime(
                                            "%Y-%m-%d %H:%M:%S") + " Unzip file")
                                        StatusZip = Unzip(
                                            FileName, Extension, Password, SourcePath, DestinationPath, IPServer, isMoveToBackUp, BackupPath)
                                    # else : //if str(Action) == 'UNZIP':
                                    # 	print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> Zip file")

                                    if StatusZip['rc']:
                                        updateLogActivity(str(IPServer), str(
                                            No), str('1'), str(StatusZip['result']))
                                    else:
                                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Gagal "+str(
                                            Action)+" file tanggal " + str(Filedate) + " untuk Param " + str(ID) + " Fitur " + str(IDFitur))
                                except Exception as e:
                                    message = traceback.format_exc()
                                    print(message)
                                    ConFitur.rollback()
                                finally:
                                    CurFitur.close()
                            else:
                                print(datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + " >> Thread-"+str(counter)+" Kriteria LogActivity belum terpenuhi")
                    else:
                        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") +
                              " << tidak ada Tanggal yang perlu diproses untuk Param " + str(ID) + " Fitur " + str(IDFitur))
                else:
                    print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " << Parameter koneksi untuk Param " +
                          str(ID) + " Fitur " + str(IDFitur)+" tidak ditemukan")
            except Exception as e:
                message = traceback.format_exc()
                print(message)
                continue
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") +
              " << Selesai memproses Fitur yang terdaftar")
    else:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") +
              " << tidak ditemukan Fitur yang terdaftar")


def getFitur(counter):
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        # Get registered Fitur
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") +
              " >> Thread-"+str(counter)+" mengambil Fitur yang terdaftar")
        qryGetFitur = ("""select
							ID, IDFitur, Action, SourcePath, FileName, Extension, Password, DestinationPath, isMoveToBackUp, BackupPath, IPServer, LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDateAdd
						FROM dbo.ParamZipFile with(nolock)
						where IDFitur = 9501101 and isActive=1 and IDFitur%{TotalThread} = {counter}""")

        curDataSourceDistributor.execute(qryGetFitur.format(
            TotalThread=sanitize(Config.threads), counter=sanitize(counter)))
        Fiturs = curDataSourceDistributor.fetchall()
        return Fiturs
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return []
    finally:
        curDataSourceDistributor.close()


def cekCriteria(IDFitur, YYYYMMDD, IPServer):
    try:
        conDataSourceDistributor = ConnectDB.connect_DSD()
        curDataSourceDistributor = conDataSourceDistributor.cursor()

        qryFiturCekLogActivity = ("""select
										LogActivityModul, LogActivitySubModul, LogActivityEntity, LogActivityDateAdd
									from dbo.ParamZipFileCekLogActivity with(nolock)
									where IDFitur={IDFitur} and isActive=1 """)
        # print(qryFiturCekLogActivity.format(IDFitur=sanitize(IDFitur)))
        curDataSourceDistributor.execute(qryFiturCekLogActivity.format(
            IDFitur=int(sanitize(IDFitur))))
        FiturCekLogActivity = curDataSourceDistributor.fetchall()
        CekLogActivityTrue = False
        if len(FiturCekLogActivity) > 0:
            for CekLogActivity in FiturCekLogActivity:
                cekLogActivityModul, cekLogActivitySubModul, cekLogActivityEntity, LogActivityDateAdd = CekLogActivity

                toDataDate = datetime.strptime(YYYYMMDD, "%Y%m%d")
                Tanggal = toDataDate + timedelta(days=LogActivityDateAdd)

                cekDone = cekLogActivity(
                    IPServer, cekLogActivityModul, cekLogActivitySubModul, cekLogActivityEntity, Tanggal.strftime("%Y%m%d"))
                if cekDone['rc']:
                    for cek in cekDone['result']:
                        if str(cek[6]) == '1':
                            CekLogActivityTrue = True
                        else:
                            CekLogActivityTrue = False
                            break
                if CekLogActivityTrue == False:
                    break
        else:
            CekLogActivityTrue = True
        return CekLogActivityTrue
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return False
    finally:
        curDataSourceDistributor.close()


def replaceDateParam(DataDate, DateAdd, stringtoreplace):
    formatTgl = re.sub('[\{\}]', '', stringtoreplace)
    toDataDate = datetime.strptime(DataDate, "%Y%m%d")
    Tanggal = toDataDate + timedelta(days=DateAdd)
    return Tanggal.strftime(formatTgl)


def Unzip(FileName, Extension, Password, SourcePath, DestinationPath, IPServer, isMoveToBackUp, BackupPath):
    try:
        Password = passDecrypt(Password, IPServer+Config.saltKeyadd)
        system = subprocess.call([Config.Conn7z['path'], "x", SourcePath+FileName+Extension, "-p"+Password, "-o"+DestinationPath])
        # else:
        # 	Archive(SourcePath+FileName+Extension).extractall(DestinationPath)

        if(isMoveToBackUp == 1):
            move = shutil.move(SourcePath+FileName+Extension, BackupPath+FileName+Extension)
        else:
            delete = os.remove(SourcePath+FileName+Extension)

        return {'rc': True, 'result': str(1)}

    except Exception as e:
        print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >> "+str(e))
        print(traceback.format_exc())
        return False


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
