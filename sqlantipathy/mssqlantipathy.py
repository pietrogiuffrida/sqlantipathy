#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import struct
import numpy as np
import pyodbc
from datetime import datetime, timezone, timedelta
import logging

from . import SqlAntipathy

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class MssqlAntipathy(SqlAntipathy):
    """Libreria per gestire la connessione a un server MSSQl"""

    connection_string = "DRIVER={{{driver}}};SERVER={hostname}"

    show_tables_query = """SELECT Distinct TABLE_NAME FROM information_schema.TABLES"""
    show_databases_query = """select * from sys.databases WHERE name NOT IN('master', 'tempdb', 'model', 'msdb');"""
    insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""

    bulk_insert_statement = """INSERT INTO {0} ({1}) VALUES {2}"""

    def __init__(self, hostname, user, password, trusted_connection=False, driver=None,
                 autocommit=False, timeout=10, datetime_converter=True, connect=False):
        """Definizione dei parametri necessari per la connessione al server MSSQL
        
        Args:
            hostname (str): L'hostname del server MSSQL a cui connettersi
            user (str, optional): Username per accesso a server MSSQL. Defaults to None.
            password (str, optional): Password per accesso a server MSSQL. Defaults to None.
            trusted_connection (bool, optional): Connessione tramite autenticazione windows. Defaults to False.
            driver (str, optional): Defaults to None.
            autocommit (bool, optional): Defaults to False.
            timeout (int, optional): durata massima di un tentaivo di connessione (in secondi). Defaults to 10.
            datetime_converter (bool, optional): Defaults to True.
        """

        super().__init__(hostname, user, password, timeout, connect)
        logger.debug("Creating an instance of sqlConnection")

        self.trusted_connection = trusted_connection

        self.autocommit = autocommit
        self.timeout = timeout
        self.datetime_converter = datetime_converter

        self.driver = self._get_driver(driver)
        self.connection_string = self.make_connection_string()

    def open_connection(self):

        try:
            logger.debug("Tring to connect")
            connection = pyodbc.connect(self.connection_string, timeout=self.timeout)
        except:
            logger.error("COULD NOT PERFORM CONNECTION TO DB")
            logger.exception("")
            raise ConnectionError("Connection failed!")

        if self.datetime_converter:
            logger.debug("Enabling datetime_converter")
            connection.add_output_converter(-155, self._handle_datetimeoffset)

        if self.autocommit:
            logger.debug("Enabling autocommit")
            connection.autocommit = True

        return connection

    def _get_driver(self, driver):
        """Funzione che permette di ottenere il driver
        
        Arguments:
            driver (str): Nome del driver richiesto
                    
        Raises:
            ValueError: Restitusce errore se il driver è un oggetto di tipo None
        
        Returns:
            str: Restituisce il percorso del driver
        """

        drivers = {
            "redhat": "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1",
            "windows": "SQL Server",
            "pietro_windows": "ODBC Driver 17 for SQL Server",
        }

        if driver is None:
            raise ValueError("driver parameter cannot be None")

        if driver in drivers:
            driver = drivers[driver]

        logger.debug("Using driver {}".format(driver))
        return driver

    def make_connection_string(self):
        """Crea stringa contenente credenziali per accedere al server MSSQL
        
        Raises:
            ValueError: Restituisce errore se sono contemporaneamente impostate la connessione tramite username/password e autenticazione windows
        
        Returns:
            str: Restituisce stringa di connessione
        """

        mssql_connection_string = self.connection_string.format(
            driver=self.driver, hostname=self.hostname
        )

        if self.trusted_connection and (self.user or self.password):
            logger.warning(
                "You must specify only one between trusted_connection and user/password"
            )
            raise ValueError(
                "You must specify only one between trusted_connection and user/password"
            )

        if self.trusted_connection:
            mssql_connection_string += ";Trusted_Connection=yes"

        if self.user:
            mssql_connection_string += ";UID={user}".format(user=self.user)

        if self.password:
            mssql_connection_string += ";password={password}".format(password=self.password)

        logger.debug("Connection string: {}".format(mssql_connection_string))

        return mssql_connection_string

    def retrieve_table(self, dbname, qry, json_fields=[]):
        """Run the query and returns a list of dict

        It overwrites original retrieve_table to enable json field parsing.

        Args:
            dbname (str):
            qry (str):
            json_fields (list, optional):

        Returns:
            list of dict
        """
        logger.debug("Parsing data")
        values = self.retrieve(dbname=dbname, qry=qry)
        keys = [i[0] for i in self.cursor.description]

        data = []
        for record in values:
            parsed = {}
            for key, value in zip(keys, record):
                if key in json_fields:
                    parsed[key] = json.loads(value)
                else:
                    parsed[key] = value
            data.append(parsed)

        logger.debug("Data parsed")

        return data

    def bulk_insertion(self, list_of_columns, data_as_dict,
                       dbname, table_name, commit_every=5000,
                       record_each_statement=200):
        """

        Args:
            list_of_columns:
            data_as_dict:
            dbname:
            table_name:
            commit_every:
            record_each_statement:

        Returns:

        """

        logger.debug("bulk_insertion {0}.{1}".format(dbname, table_name))

        self.use_database(dbname)

        len_data = len(data_as_dict)
        executions = 0

        try:
            multiple_values = []
            for idx, row in enumerate(data_as_dict):

                _, values = self.make_list_of_values(
                    row, list_of_columns=list_of_columns
                )

                multiple_values.append("(" + values + ")")

                if (idx > 0 and idx % record_each_statement == 0 or
                        idx + 1 == len_data):

                    statement = self.bulk_insert_statement.format(
                        table_name,
                        ", ".join(list_of_columns),
                        ", ".join(multiple_values),
                    )

                    try:
                        self.cursor.execute(statement)
                        multiple_values = []
                        executions += 1

                    except:
                        logger.error("Errore a idx {0}".format(idx + 1 ))
                        logger.error("Statement {}".format(statement))
                        logger.exception("")
                        return 1

                if (idx > 0 and idx % commit_every == 0 or
                        idx + 1 == len_data):
                    logger.info(
                        "Arrivato a {}/{} ({} executions)".format(
                            idx, len_data, executions
                        )
                    )
                    self.connection.commit()

            self.connection.commit()
            return 0

        except:
            logger.error("CARICAMENTO DATI FALLITO!!!")
            logger.error("Last row ({0}) {1}".format(idx, row or None))
            logger.exception("")
            return 1

    def _handle_datetimeoffset(dto_value):
        # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
        # https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
        tup = struct.unpack(
            "<6hI2h", dto_value
        )  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
        return datetime(
            tup[0],
            tup[1],
            tup[2],
            tup[3],
            tup[4],
            tup[5],
            tup[6] // 1000,
            timezone(timedelta(hours=tup[7], minutes=tup[8])),
        )

    def sql_clean(self, value):
        """Pulisce valori nulli e stringhe.

        Ricevuto il valore da inserire nel db, esegue dei controlli per codificare
        correttamente l'informazione NULL, a partire dai diversi formati logici in cui
        essa può essere codificata in python.

        Args:
            value (str or int or None): il valore da pulire

        Returns:
            Restituisce il valore "NULL," se input è valore nullo, stringa vuota o None
        """

        if type(value) == float and np.isnan(value):
            return "NULL,"
        if value in ["", "NULL", None]:
            return "NULL,"
        if type(value) == str:
            value = re.sub(r'"*|:?\\+|/', "", value).strip()
            value = value.replace("'", "''")
        return "'{}',".format(value)