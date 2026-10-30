#!/usr/bin/python
import Config
import sys
import pyodbc
import os
import traceback
import re
import logging

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
        logging.exception("error : "+message)
    #print("error :"+ message)

def checkColumnExist(cursor, TableName, ColumnList):
    try:
        isexist = True
        for columnName in ColumnList :
            qryCheck = ("""SELECT count(1) as cek FROM sys.columns with(nolock) WHERE (Name = N'{columnName}' or '['+Name+']' = N'{columnName}') AND Object_ID = Object_ID(N'dbo.{TableName}')""".format(columnName=columnName, TableName=TableName))
            cursor.execute(qryCheck)
            Check = cursor.fetchone()
            if int(Check[0])==0 :
                print(TableName, columnName)
                isexist=False
                break
        return isexist
    except Exception as e:
        message = traceback.format_exc()
        print(message)
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
            decrypted = decrypted+chr((ord(toDecrypt[i:i+1]) - (adds % 128) + 128) % 128)
            i = i+1
        return decrypted
    except Exception as e:
        return False

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
