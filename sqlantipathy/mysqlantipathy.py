#!/usr/bin/env python3

import base64
from time import sleep
from collections import Counter
import os
import re
import json
import mysql.connector as mdb
import os
# import fiona
import logging
import numpy as np

insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""

def dbLoad_single(cursor, values, table, insert_statement=insert_statement, fields=None, rownumber=0, filename=''):
  try:
    if fields != None:
      values = {k: v for k, v in zip(fields, values)}
    fields, values = make_list_of_values(values)
    statement = insert_statement.format(table, fields, values)
    cursor.execute(statement)
  except:
    logging.error('dbLoad_single error: {0} row {1}'.format(filename, rownumber, insert_statement))
    logging.error('{0}: {1}'.format(rownumber, statement))
    logging.exception("")
    return 1
  return 0

def makeConnection(dbConf, dbName):
  try:
    logging.debug('Connesione al db')
    connection = mdb.connect(host=dbConf['address'],
                             user=dbConf['user'],
                             password=dbConf['password'],
                             database=dbName,
                             );
    cursor = connection.cursor()
  except:
    logging.error('IMPOSSIBILE CONNETTERSI AL SERVER MySQL')
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
        list_of_columns, list_of_values = make_list_of_values(row, ukey=True)
      else:
        list_of_columns, list_of_values = make_list_of_values(row, ukey=False)

      try:
        statement = """INSERT INTO {0} ({1}) VALUES ({2})""".format(table_name, list_of_columns, list_of_values)
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



def mysqlClean(value):
  if type(value) == float and np.isnan(value):
    return 'NULL,'
  if value in ["", 'NULL', None]:
    return 'NULL,'
  if type(value) == str:
    value = re.sub(r'"*|:?\\+|/', '', value).strip()
  return '"{}",'.format(value)


def make_list_of_values(values_dict, ukey=False):
  values = ''
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


def checkIban(cursor, iban):
  try:
    # Verifico se l'iban è noto, restituisco l'informazione completa
    qry = """SELECT zcode, iban, abi, ndg FROM conti_alternativi WHERE iban = '{0}'""".format(iban)
    cc = retrieve(qry, cursor)
  except:
    logging.error('insertCC error: {}'.format(statement))
    logging.exception("")
  return len(cc), cc


def checkAbiNdgCC(cursor, abi, ndg):
  try:
    # Verifico se abi/ndg sono noti, restituisco l'informazione completa
    qry = """SELECT zcode, abi, ndg FROM conti_alternativi WHERE iban = '{0}'""".format(abi, ndg)
    cc = retrieve(qry, cursor)
  except:
    logging.error('checkAbiNdg error: {}'.format(qry))
    logging.exception("")
  return len(cc), cc


def insertCC(cursor, fields, values, insert_statement=insert_statement):
  try:
    d = {k: v for k, v in zip(fields, values)}
    logging.debug(d)
    list_of_columns, list_of_values = make_list_of_values(d)
    statement = insert_statement.format('conti_alternativi', list_of_columns, list_of_values)
    logging.debug(statement)
    cursor.execute(statement)
  except:
    logging.error('insertCC error: {}'.format(statement))
    logging.exception("")
  return


def appendRS(cursor, data):
  try:
    data['id_cc'] = data.get('id_cc')
    data['ragione_sociale'] = re.sub(r'"*', '', data['ragione_sociale'])
    data['iniziale'] = 0
    list_of_columns, list_of_values = make_list_of_values(data)
    qry = """INSERT IGNORE INTO ragioni_sociali_alternative ({0}) VALUES ({1})""".format(list_of_columns, list_of_values)
    cursor.execute(qry)
  except:
    logging.error('appendRS error: {}'.format(qry))
    logging.exception("")
  return


def recupero_rotti(dati_cc, sintesi, zcode, cursor):

  if zcode == None:
    logging.debug('Lo zcode è nullo!! Non Recupero!!')
    return sintesi

  iban_rotti = [i['iban'] for i in dati_cc if i['zcode'] in ['', None, 'NULL', 'Null']]

  if len(iban_rotti) > 0:
    logging.info('Recupero {0} {1}'.format(iban_rotti, zcode))
    sintesi['recg'] += 1
    logging.debug('Recupero {} iban'.format(len(iban_rotti)))
    for rotto in iban_rotti:
      sintesi['rec'] += 1
      try:
        qry = """UPDATE {0} SET zcode = {1} WHERE iban = '{2}'""".format("conti_alternativi", zcode, rotto)
        cursor.execute(qry)
      except:
        logging.error('Errore recupero_rotti {}'.format(qry))
        logging.exception('')
        os._exit(1)
  return sintesi
