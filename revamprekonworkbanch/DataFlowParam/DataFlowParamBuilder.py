import Config
import sys
import pyodbc
import os
import traceback
from datetime import timedelta, date, datetime
from Etc import sanitize, passDecrypt
import re

def getSourceSP(DBType, IPServer, DBPort, DBUser, DBPass,DBName, ApplicationIntent, InitialCatalog,StoredProcedureExecute) :
    try :
        isDBTypeExist = False
        #MSSQL
        if(str(DBType).lower()=="mssql") :
            ApplicationIntentConnection = ''
            InitialCatalogConnection = ''
            if(len(ApplicationIntent)>0) :
                ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
            if(len(InitialCatalog)>0) :
                InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
            conDataSource = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
            conDataSource.autocommit = True
            curDataSource = conDataSource.cursor()
            qryDataSource = "EXEC {StoredProcedureExecute}"

            isDBTypeExist = True
        #MySQL
        if(str(DBType).lower()=="mysql") :
            conDataSource = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MySQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
            conDataSource.autocommit = True
            curDataSource = conDataSource.cursor()
            qryDataSource = "CALL {StoredProcedureExecute}"
            isDBTypeExist = True

        if(isDBTypeExist) :
            curDataSource.execute(qryDataSource.format(StoredProcedureExecute=sanitize(StoredProcedureExecute)))
            return {'rc' : True, 'result' : curDataSource, 'headerRaw' : curDataSource.description, 'count' : curDataSource.rowcount}
        else :
            return {'rc' : False, 'msg' : " DBType tidak ditemukan"}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : " exception"}



def getSourceSelect(DBType, IPServer, DBPort, DBUser, DBPass,DBName, ApplicationIntent, InitialCatalog,TableNameA,TableNameB,TableNameC,TableNameD,AJoinBType,ABJoinCType,ABCJoinDType,AJoinBKey,ABJoinCKey,ABCJoinDKey,FieldSelect,WhereCondition,GroupByCondition,HavingCondition,OrderByCondition) :
    try :
        isDBTypeExist = False
        #MSSQL
        if(str(DBType).lower()=="mssql") :
            ApplicationIntentConnection = ''
            InitialCatalogConnection = ''
            if(len(ApplicationIntent)>0) :
                ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
            if(len(InitialCatalog)>0) :
                InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
            conDataSource = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
            curDataSource = conDataSource.cursor()

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

            #group by
            GroupByConditionName = ""
            if(len(GroupByCondition)>0) :
                GroupByConditionName = "GROUP BY "+GroupByCondition

            #having
            HavingConditionName = ""
            if(len(HavingCondition)>0) :
                HavingConditionName = "HAVING "+HavingCondition

            #order
            OrderByConditionName = ""
            if(len(OrderByCondition)>0) :
                OrderByConditionName = "ORDER BY "+OrderByCondition

            ##query builder
            qryDataSource = "SELECT {FieldSelect} FROM {TableSourceName} {WhereConditionName} {GroupByConditionName} {HavingConditionName} {OrderByConditionName}"

            isDBTypeExist = True

        #MySQL
        if(str(DBType).lower()=="mysql") :
            conDataSource = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MySQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
            curDataSource = conDataSource.cursor()

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

            #group by
            GroupByConditionName = ""
            if(len(GroupByCondition)>0) :
                GroupByConditionName = "GROUP BY "+GroupByCondition

            #having
            HavingConditionName = ""
            if(len(HavingCondition)>0) :
                HavingConditionName = "HAVING "+HavingCondition

            #order
            OrderByConditionName = ""
            if(len(OrderByCondition)>0) :
                OrderByConditionName = "ORDER BY "+OrderByCondition

            ##query builder
            qryDataSource = "SELECT {FieldSelect} FROM {TableSourceName} {WhereConditionName} {GroupByConditionName} {HavingConditionName} {OrderByConditionName}"
            #print(qryDataSource)
            isDBTypeExist = True



        if(isDBTypeExist) :
            curDataSource.execute(qryDataSource.format(FieldSelect=sanitize(FieldSelect),TableSourceName=sanitize(TableSourceName),WhereConditionName=sanitize(WhereConditionName),GroupByConditionName=sanitize(GroupByConditionName),HavingConditionName=sanitize(HavingConditionName),OrderByConditionName=sanitize(OrderByConditionName)))
            return {'rc' : True, 'result' : curDataSource, 'headerRaw' : curDataSource.description, 'count' : curDataSource.rowcount}
        else :
            return {'rc' : False, 'msg' : " DBType tidak ditemukan"}
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False, 'msg' : " exception"}



def insertDestination(DBType, IPServer, DBPort, DBUser, DBPass,DBName, ApplicationIntent, InitialCatalog, TableName, headerRaw, dataInsert,isSPExecute,FieldSelect):
    try:
        isDBTypeExist = False
        #generate header / columnname

        if(isSPExecute and len(FieldSelect)>0) :
            FieldSelectList = FieldSelect.split(",")
            headerasli = genColumnNameListSelect(DBType,headerRaw)
            header = genColumnNameListSP(DBType,FieldSelectList)
            cekColumnSelectExistinResult = all(elem in headerasli for elem in header)
            if(not cekColumnSelectExistinResult) :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<< Gagal validasi kolom source table")
                return {'rc':False, 'result':str('Gagal validasi kolom tabel tujuan')}
            isSPSelectSomeColumn = True
        else	:
            FieldSelectList = []
            header = genColumnNameListSelect(DBType,headerRaw)
            isSPSelectSomeColumn = False
        #getfieldsname
        fieldsCursor = getFieldsIndex(dataInsert)

        #mssql
        if(str(DBType).lower()=="mssql") :
            ApplicationIntentConnection = ''
            InitialCatalogConnection = ''
            if(len(ApplicationIntent)>0) :
                ApplicationIntentConnection = ';ApplicationIntent='+str(ApplicationIntent)
            if(len(InitialCatalog)>0) :
                InitialCatalogConnection = ';Initial Catalog='+str(InitialCatalog)
            isDBTypeExist = True
            conDataDestination = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MSSQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd))+ApplicationIntentConnection+InitialCatalogConnection)
            conDataDestination.autocommit = False
            curDataDestination = conDataDestination.cursor()

            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>>> validasi kolom tabel tujuan server "+str(IPServer)+" DB "+str(DBName))

            ColumnExist = checkColumnDestinationExist(curDataDestination,DBType,DBName, TableName, header)
            #print(header)
            if ColumnExist :
                colNames = ','.join(header)
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>>> Inserting data to destination . . .")
                qryInsertDestination = ("""Insert into {TableName} ({colNames}) values ("""+ ','.join(['?'] * len(header)) + """)""")
                #print(qryInsertDestination)

                curDataDestination.fast_executemany = True
                #print(qryInsertDestination.format(TableName=sanitize(TableName), colNames=sanitize(colNames)), row_gen(dataInsert))
                curDataDestination.executemany(qryInsertDestination.format(TableName=sanitize(TableName), colNames=sanitize(colNames)), row_gen(dataInsert,isSPSelectSomeColumn,fieldsCursor,FieldSelectList))


                conDataDestination.commit()
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<<< Selesai insert data")
                return {'rc':True, 'result':str(curDataDestination.rowcount)}
            else :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<<< Gagal validasi kolom tabel tujuan")
                return {'rc':False, 'result':str('Gagal validasi kolom tabel tujuan')}
            curDataDestination.close()
        #mysql
        if(str(DBType).lower()=="mysql") :
            isDBTypeExist = True
            conDataDestination = pyodbc.connect("DRIVER={"+str(Config.ODBCDriver['MySQL'])+"};SERVER="+str(IPServer)+";PORT="+str(DBPort)+";DATABASE="+str(DBName)+";UID="+str(DBUser)+";PWD="+str(passDecrypt(DBPass, IPServer+Config.saltKeyadd)))
            conDataDestination.autocommit = False
            curDataDestination = conDataDestination.cursor()

            print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>>> validasi kolom tabel tujuan server "+str(IPServer)+" DB "+str(DBName))


            ColumnExist = checkColumnDestinationExist(curDataDestination, DBType, DBName,TableName, header)
            #print(header)
            if ColumnExist :
                colNames = ','.join(header)
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " >>>> Inserting data to destination . . .")
                qryInsertDestination = ("""Insert into {TableName} ({colNames}) values ("""+ ','.join(['?'] * len(header)) + """)""")

                curDataDestination.fast_executemany = True
                curDataDestination.executemany(qryInsertDestination.format(TableName=sanitize(TableName), colNames=sanitize(colNames)), row_gen(dataInsert,isSPSelectSomeColumn,fieldsCursor,FieldSelectList))
                conDataDestination.commit()
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<<< Selesai insert data")
                return {'rc':True, 'result':str(curDataDestination.rowcount)}
            else :
                print(datetime.today().strftime("%Y-%m-%d %H:%M:%S") + " <<<< Gagal validasi kolom tabel tujuan")
                return {'rc':False, 'result':str('Gagal validasi kolom tabel tujuan')}
            curDataDestination.close()
    except Exception as e:
        message = traceback.format_exc()
        print(message)

        conDataDestination.rollback()
        return {'rc':False, 'result': str(message)}
    finally :
        if isDBTypeExist :
            curDataDestination.close()

#def checkColumnSPExist(header,FieldSelectList) :
#colNameList = []
#colNames = str('')
#if(str(DBType).lower()=="mssql") :
#	openidentifier = "["
#	closeidentifier = "]"
#elif(str(DBType).lower()=="mysql") :
#	openidentifier = "`"
#	closeidentifier = "`"
#else :
#	openidentifier = ""
#	closeidentifier = ""
#for i in range(len(cursorDescription)):
#	desc = cursorDescription[i]
#	colNameList.append(str(openidentifier)+desc[0]+str(closeidentifier))
#return colNameList

def checkColumnDestinationExist(cursor, DBType,DBName, TableName, ColumnList):
    try:
        isDBTypeExist = False
        #mssql
        if(str(DBType).lower()=="mssql") :
            qryCheckColumnExist = "SELECT count(1) as cek FROM sys.columns with(nolock) WHERE (Name = N'{columnName}' or '['+Name+']' = N'{columnName}') AND Object_ID = Object_ID(N'dbo.{TableName}')"
            isDBTypeExist = True
        if(str(DBType).lower()=="mysql") :
            qryCheckColumnExist = "select count(1) from information_schema.COLUMNS where TABLE_SCHEMA='{DBName}' and TABLE_NAME='{TableName}' and (COLUMN_NAME='{columnName}' or '['+COLUMN_NAME+']' = '{columnName}')"
            isDBTypeExist = True
        if (not isDBTypeExist) :
            return False

        isexist = True
        for columnName in ColumnList :
            #print(qryCheckColumnExist.format(DBName=sanitize(DBName),TableName=sanitize(TableName),columnName=sanitize(columnName)))
            cursor.execute(qryCheckColumnExist.format(DBName=sanitize(DBName),TableName=sanitize(TableName),columnName=sanitize(columnName)))
            Check = cursor.fetchone()
            if int(Check[0])==0 :
                isexist=False
                break
        return isexist
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return False

def genColumnNameListSelect(DBType,cursorDescription) :
    colNameList = []
    colNames = str('')
    if(str(DBType).lower()=="mssql") :
        openidentifier = "["
        closeidentifier = "]"
    elif(str(DBType).lower()=="mysql") :
        openidentifier = "`"
        closeidentifier = "`"
    else :
        openidentifier = ""
        closeidentifier = ""
    for i in range(len(cursorDescription)):
        desc = cursorDescription[i]
        colNameList.append(str(openidentifier)+desc[0]+str(closeidentifier))
    return colNameList

def genColumnNameListSP(DBType,FieldSelectList) :
    colNameList = []
    colNames = str('')
    if(str(DBType).lower()=="mssql") :
        openidentifier = "["
        closeidentifier = "]"
    elif(str(DBType).lower()=="mysql") :
        openidentifier = "`"
        closeidentifier = "`"
    else :
        openidentifier = ""
        closeidentifier = ""
    for i in FieldSelectList:
        colNameList.append(str(openidentifier)+str(i).strip()+str(closeidentifier))
    return colNameList

def row_gen(cursor,isSPSelectSomeColumn,fieldsCursor,FieldSelectList, some=Config.batchbuffer):
    try:

        fetch = cursor.fetchmany
        if isSPSelectSomeColumn :
            while True:
                rows = fetch(some)
                if not rows: break
                for row in rows:
                    rowx = []
                    for FieldSelectListName in FieldSelectList :
                        rowx.append(row[fieldsCursor[FieldSelectListName]])
                    rowxt = tuple(rowx)
                    yield rowxt
        else :
            while True:
                rows = fetch(some)
                if not rows: break
                for row in rows:
                    yield row
    except Exception as e:
        message = traceback.format_exc()
        print(message)
    finally :
        cursor.close()

def getFieldsIndex(cursor) :
    results = {}
    column = 0
    for d in cursor.description :
        results[d[0]] = column
        column = column +1

    return results

