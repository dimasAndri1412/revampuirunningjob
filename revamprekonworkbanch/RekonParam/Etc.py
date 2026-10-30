#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
import re
from datetime import timedelta, date, datetime

def addAliasTable(aliastable, strcolumn) :
    return re.sub(r'(?is)\[[a-zA-Z0-9]', aliastable+r'.\g<0>', strcolumn)

def joinQry(joinstring, alias1, q1, alias2, q2) :
    joins = []
    for i, val in enumerate(q1) :
        if len(q1[i])>0 and len(q2[i])>0 :
            joins.append(addAliasTable(alias1, q1[i]) + " = " + addAliasTable(alias2, q2[i]))
    if len(joins) > 0 :
        strjoin = (" "+str(joinstring)+" ").join(joins)
    else :
        strjoin = str("")
    return strjoin

def row_gen(cursor, some=Config.batchbuffer):
    try:
        fetch = cursor.fetchmany
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

def checkColumnExist(cursor, TableName, ColumnList):
    try:
        isexist = True
        for columnName in ColumnList :
            qryCheck = ("SELECT count(1) as cek FROM sys.columns with(nolock) WHERE (Name = N'"+ str(columnName) +"' or '['+Name+']' = N'"+ str(columnName) +"') AND Object_ID = Object_ID(N'dbo."+ str(TableName) +"')")
            cursor.execute(qryCheck)
            Check = cursor.fetchone()
            if int(Check[0])==0 :
                isexist=False
                break
        return isexist
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return False

def createLog(IDFitur, IDTask, Note) :
    try:
        ConRekonParam = pyodbc.connect("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+str(Config.ConnRekonParam['host'])+";DATABASE="+str(Config.ConnRekonParam['database'])+";UID="+str(Config.ConnRekonParam['user'])+";PWD="+str(Config.ConnRekonParam['password']))
        ConRekonParam.autocommit = True
        CurRekonParam = ConRekonParam.cursor()

        qryCreateLog = ("Insert into LogExecute (DateExec, IDFitur, IDTask, Note) values (GetDate(), ?, ?, ?)")
        CurRekonParam.execute(qryCreateLog, (IDFitur,IDTask,Note))
    except Exception as e:
        message = traceback.format_exc()
        print(message)
        return {'rc' : False}
    finally :
        CurRekonParam.close()

def sanitize(qrystring):
    try:
        # pattern ';+(query sisipan)+(; atau --)
        cek = re.match(r"\'[\s]*\;[\s\d\w\[\]\.\,\=\'\(\)\_]*[\;|(\-\-)]+", str(qrystring))
        if cek==None:
            return qrystring
        else:
            return False
    except Exception as e:
        return False

def passDecrypt(toDecrypt, keyDecryptor) :
    try:
        i = int(0)
        adds = int(0)
        while i<len(keyDecryptor) :
            adds = adds + ord(keyDecryptor[i:i+1])
            i = i+1
        i = int(0)
        decrypted = str('')
        while i<len(toDecrypt) :
            decrypted = decrypted+chr((ord(toDecrypt[i:i+1]) - (adds % 256) + 256) % 256)
            i = i+1
        return decrypted
    except Exception as e:
        return False