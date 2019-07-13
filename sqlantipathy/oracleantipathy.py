#!/usr/bin/env python3

import base64
from time import sleep
from collections import Counter
import os
import re
import json
import cx_Oracle as mdb
import os
import csv
# import fiona
import logging
import numpy as np

insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""


def multiload(cursor, values, table, fields, rownumber=0, filename=''):
    insert_statement = """INTO {0} ({1}) VALUES ({2})"""

    try:
        if fields != None:
            values = {k: v for k, v in zip(fields, values)}
        fields, values = make_string_of_values(values)
        insert_statement = insert_statement.format(table, fields, values)
    except:
        logging.error('dbLoad_single error: {0} row {1}'.format(filename, rownumber))
        logging.error('{0}: {1}'.format(rownumber, insert_statement or None))
        logging.exception("")
        return 1, ''
    return 0, insert_statement


def dbLoad_single(cursor, values, table, insert_statement=insert_statement, fields=None, rownumber=0,
                  filename=''):
    try:
        if fields != None:
            values = {k: v for k, v in zip(fields, values)}
        fields, values = make_string_of_values(values)
        statement = insert_statement.format(table, fields, values)
        cursor.execute(statement)
    except:
        logging.error('dbLoad_single error: {0} row {1}'.format(filename, rownumber, insert_statement))
        logging.error('{0}: {1}'.format(rownumber, statement))
        logging.exception("")
        return 1
    return 0


def makeConnection(dbConf):
    try:
        logging.debug('Connesione al db')
        connection = mdb.connect('{0}/{1}@{2}'.format(dbConf["user"], dbConf["pwd"], dbConf["server"]));
        cursor = connection.cursor()
    except:
        logging.error('IMPOSSIBILE CONNETTERSI AL SERVER ORACLE')
        logging.exception('')
        os._exit(1)
    return connection, cursor


def dbLoad(data, dbConf, table_name, dbName, make_unique_key=False):
    logging.debug('Caricamento dati {}'.format(table_name))
    connection, cursor = makeConnection(dbConf, dbName)
    ldata = len(data)
    statement = "statement primo giro"
    gestione = Counter()
    try:
        for idx, row in enumerate(data):

            if idx % 50000 == 0:
                logging.info('Arrivato a {0}/{1}'.format(idx, ldata))

            if make_unique_key == True:
                list_of_columns, list_of_values = make_string_of_values(row, ukey=True)
            else:
                list_of_columns, list_of_values = make_string_of_values(row, ukey=False)

            try:
                statement = """INSERT INTO {0} ({1}) VALUES ({2})""".format(table_name, list_of_columns,
                                                                            list_of_values)
                cursor.execute(statement)
            except mdb.IntegrityError:
                logging.debug("Duplicato! (IntegrityError)")
                gestione['IntegrityError'] += 1
                continue
            except:
                logging.error('Errore a idx {0}, dati {1}'.format(idx, list_of_values))
                logging.exception('')
                sleep(20)
                continue

        connection.commit()
        connection.close()
        logging.info('dbLoad report:\n {}'.format(gestione))
        return 0
    except:
        logging.error('CARICAMENTO DATI FALLITO!!!')
        logging.error(row)
        logging.info(statement)
        logging.exception('')
        logging.info('dbLoad report:\n {}'.format(gestione))
        return 1


def valueClean(value):
    if type(value) == str and 'to_date' in value:
        return value
    if type(value) == float and np.isnan(value):
        return None
    if type(value) == float:
        return value
    if value in ["", 'NULL', None]:
        return None
    if type(value) == str:
        value = re.sub(r'"*|:?\\+|/', '', value).strip()
        value = re.sub(r"'", "''", value).strip()
        value = re.sub(r"&", "AND", value).strip()
    return value


def mysqlClean(value):
    if type(value) == str and 'to_date' in value:
        return "{},".format(value)
    if type(value) == float and np.isnan(value):
        return 'NULL,'
    if type(value) == float:
        return "{},".format(value)
    if value in ["", 'NULL', None]:
        return 'NULL,'
    if type(value) == str:
        value = re.sub(r'"*|:?\\+|/', '', value).strip()
        value = re.sub(r"'", "''", value).strip()
        value = re.sub(r"&", "AND", value).strip()
    return "'{}',".format(value)


def make_string_of_values(values_dict, ukey=False):
    values = ""
    columns = ''
    for key in values_dict:
        values += mysqlClean(values_dict[key])
        key += ','
        columns += key

    if ukey == True:
        ukey = base64.encodebytes(bytes(values, 'utf8'))
        values += mysqlClean(ukey.decode('utf8'))
        columns += 'ukey'

    values = values.strip(',')
    columns = columns.strip(',')

    return columns, values


def make_list_of_values(values):
    vs = []
    for value in values:
        vs.append(valueClean(value))
    return vs


# trasforma i vettori fecthall in campi parlanti
def retrieve(qry, cursor):
    try:
        cursor.execute(qry)
    except:
        logging.error('retrieve error: {}'.format(qry))
        logging.exception("")
    keys = cursor.column_names
    values = cursor.fetchall()
    return [{k: i for k, i in zip(keys, j)} for j in values] or []


def cleanData(dizionario, chiavenulla):
    return {i: dizionario[i] for i in dizionario if dizionario[i] != chiavenulla}


def eliminaNewLine(TuplaIn):
    stringaOut = list(TuplaIn)
    for i in range(len(TuplaIn)):
        if type(stringaOut[i]) == str:
            stringaOut[i] = stringaOut[i].replace('\r\n', ' ')
            stringaOut[i] = stringaOut[i].replace('\n', ' ')
            stringaOut[i] = stringaOut[i].replace('\r', ' ')
    return stringaOut


def esporta_csv(sql, csv_file_dest, dbConf):
    connection, cursor = makeConnection(dbConf)
    # csv_file_dest = "Pea_Tabella_finale.csv"
    outputFile = open(csv_file_dest, 'w', newline='\n')  # 'wb'
    output = csv.writer(outputFile, dialect='excel')
    # sql = "select * from  Pea_Tabella_finale "
    cursor.execute(sql)
    headers = [i[0] for i in cursor.description]
    output.writerow(headers)
    for row_data in cursor:  # add table rows
        row_data = eliminaNewLine(row_data)
        output.writerow(row_data)
    outputFile.close()