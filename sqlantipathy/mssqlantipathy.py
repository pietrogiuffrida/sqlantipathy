#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import struct
import numpy as np
import pyodbc
from datetime import datetime, timezone, timedelta
import logging

from sqlantipathy import SqlAntipathy

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class MssqlAntipathy(SqlAntipathy):
    """Libreria per gestire la connessione a un server MSSQl"""

    empty_connection_string = "DRIVER={{{driver}}};SERVER={hostname}"

    show_table_schema_qry = """SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"""
    show_tables_query = """SELECT Distinct TABLE_NAME FROM information_schema.TABLES"""
    show_databases_query = """SELECT * FROM SYS.DATABASES WHERE NAME NOT IN('MASTER', 'TEMPDB', 'MODEL', 'MSDB')"""
    insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""

    bulk_insert_statement = """INSERT INTO {0} ({1}) VALUES {2}"""

    drivers = {
        'redhat': "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1",
        "windows": "SQL Server",
    }

    def __init__(
            self,
            hostname,
            user=None,
            password=None,
            trusted_connection=False,
            driver=None,
            autocommit=False,
            timeout=10,
            datetime_converter=True,
            connect=False,
    ):
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
        self.driver = self.drivers[driver]

        self.autocommit = autocommit
        self.timeout = timeout
        self.datetime_converter = datetime_converter

    def make_connection_string(self):
        """Crea stringa contenente credenziali per accedere al server MSSQL

        Returns:
            str: Restituisce stringa di connessione
        """
        self.connection_string = self.empty_connection_string.format(
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
            self.connection_string += ";Trusted_Connection=yes"

        if self.user:
            self.connection_string += ";UID={user}".format(user=self.user)

        if self.password:
            self.connection_string += ";PWD={password}".format(
                password=self.password
            )

        logger.debug("Connection string: {}".format(self.connection_string))


    def open_connection(self):
        try:
            logger.debug("Tring to connect")
            self.connection = pyodbc.connect(self.connection_string, timeout=self.timeout)
        except:
            logger.error("COULD NOT PERFORM CONNECTION TO DB")
            logger.exception("")
            raise ConnectionError("Connection failed!")

        if self.datetime_converter:
            logger.debug("Enabling datetime_converter")
            self.connection.add_output_converter(-155, self._handle_datetimeoffset)

        if self.autocommit:
            logger.debug("Enabling autocommit")
            self.connection.autocommit = True


    def retrieve_table(self, dbname, qry, json_fields=None):
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

        if json_fields is None:
            json_fields = []

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

    def bulk_insertion(
            self,
            table_name,
            list_of_columns,
            data_as_dict,
            dbname,
            record_each_statement=200,
            commit_every=5000,
    ):

        logger.debug("bulk_insertion {0}.{1}".format(dbname, table_name))

        self.use_database(dbname)

        len_data = len(data_as_dict)
        executions = 0

        idx = None
        row = None
        try:
            multiple_values = []
            for idx, row in enumerate(data_as_dict):

                _, values = self.make_list_of_values(
                    row, list_of_columns=list_of_columns
                )

                multiple_values.append("(" + values + ")")

                if idx > 0 and idx % record_each_statement == 0 or idx + 1 == len_data:

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
                        logger.error("Errore a idx {0}".format(idx + 1))
                        logger.error("Statement {}".format(statement))
                        logger.exception("")
                        return 1

                if idx > 0 and idx % commit_every == 0 or idx + 1 == len_data:
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

    def show_table_schema(self, dbname, table_name):
        schema = self.retrieve_table(
            dbname=dbname,
            qry=self.show_table_schema_qry.format(table_name=table_name)
        )
        return schema
