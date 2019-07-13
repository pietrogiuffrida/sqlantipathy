#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import struct
import numpy as np
import pyodbc
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class MssqlAntipathy:
    """Libreria per gestire la connessione a un server MSSQl"""

    def __init__(
        self,
        hostname,
        user=None,
        pwd=None,
        trusted_connection=False,
        driver=None,
        autocommit=False,
        timeout=10,
        datetime_converter=True,
        open_connection=False,
    ):
        """Definizione dei parametri necessari per la connessione al server MSSQL
        
        Args:
            hostname (str): L'hostname del server MSSQL a cui connettersi
            user (str, optional): Username per accesso a server MSSQL. Defaults to None.
            pwd (str, optional): Password per accesso a server MSSQL. Defaults to None.
            trusted_connection (bool, optional): Connessione tramite autenticazione windows. Defaults to False.
            driver (str, optional): Defaults to None.
            autocommit (bool, optional): Defaults to False.
            timeout (int, optional): durata massima di un tentaivo di connessione (in secondi). Defaults to 10.
            datetime_converter (bool, optional): Defaults to True.
        """

        logger.debug("Creating an instance of sqlConnection")

        self.insert_statement = """INSERT INTO {0} ({1}) VALUES ({2})"""
        self.bulk_insert_statement = """INSERT INTO {0} ({1}) VALUES {2}"""

        self.hostname = hostname
        self.user = user
        self.pwd = pwd
        self.trusted_connection = trusted_connection

        self.autocommit = autocommit
        self.timeout = timeout
        self.datetime_converter = datetime_converter

        self.driver = self._get_driver(driver)
        self.connection_string = self._make_connection_string()

        self.connection = None
        self.cursor = None

        if open_connection:
            self.connect()

    def connect(self):
        self.connection = self.open_connection()

        if self.datetime_converter:
            logger.debug("Enabling datetime_converter")
            self.connection.add_output_converter(-155, self._handle_datetimeoffset)

        if self.autocommit:
            logger.debug("Enabling autocommit")
            self.connection.autocommit = True

        self.cursor = self.open_cursor()

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

        if driver == None:
            raise ValueError("driver parameter cannot be None")

        if driver in drivers:
            driver = drivers[driver]

        logger.debug("Using driver {}".format(driver))
        return driver

    def _make_connection_string(self):
        """Crea stringa contenente credenziali per accedere al server MSSQL
        
        Raises:
            ValueError: Restituisce errore se sono contemporaneamente impostate la connessione tramite username/password e autenticazione windows
        
        Returns:
            str: Restituisce stringa di connessione
        """

        connection_string = "DRIVER={{{driver}}};SERVER={hostname}".format(
            driver=self.driver, hostname=self.hostname
        )

        if self.trusted_connection and (self.user or self.pwd):
            logger.warning(
                "You must specify only one between trusted_connection and user/password"
            )
            raise ValueError(
                "You must specify only one between trusted_connection and user/password"
            )

        if self.trusted_connection:
            connection_string += ";Trusted_Connection=yes"

        if self.user:
            connection_string += ";UID={user}".format(user=self.user)

        if self.pwd:
            connection_string += ";PWD={pwd}".format(pwd=self.pwd)

        logger.debug("Connection string: {}".format(connection_string))

        return connection_string

    def open_connection(self):
        """Apre la connessione al server MSSQL
        
        Raises:
            ConnectionError: Restituisce errore se la connessione fallisce
        
        Returns:
            str: Accesso al Server
        """
        try:
            logger.debug("Tring to connect")
            connection = pyodbc.connect(self.connection_string, timeout=self.timeout)
        except:
            logger.error("COULD NOT PERFORM CONNECTION TO DB")
            logger.exception("")
            raise ConnectionError("Connection failed!")
        return connection

    def close_connection(self):
        """Chiude la connessione al server MSSQL"""
        logger.debug("Closing connection")
        self.connection.close()

    def open_cursor(self):
        """Accesso al cursore del server MSSQL"""
        logger.debug("Opening cursor")
        return self.connection.cursor()

    def use_database(self, dbname):
        """Permette l'accesso a un database all'interno del server MSSQL
        
        Args:
            dbname (str): Nome del database a cui si desidera accedere
        """
        self.cursor.execute("USE {}".format(dbname))

    def retrieve(self, dbname, qry):
        """Esegue una query SQL su un database in input
        
        Args:
            dbname (str): Nome del database su cui eseguire la query
            qry (str): Query SQL che si desidera effettuare
        
        Returns:
            list: Restituisce una lista di tuple come risultato della query richiesta
        """

        self.use_database(dbname=dbname)

        logger.debug("Running query")
        self.cursor.execute(qry)

        logger.debug("Reading data")
        return self.cursor.fetchall()

    def retrieve_table(self, dbname, qry, json_fields=[]):
        """Funzione che applica una query a un database e restituisce i dati sotto forma di lista di dizionari
        
        Args:
            dbname (str): Nome del database su cui eseguire la query 
            qry (str): Query SQL che si desidera effettuare
            json_fields (list, optional): Json da cui estrarre valori . Defaults to [].
        
        Returns:
            list: Restituisce una lista di dizionari contenente i dati di nostro interesse
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

    def show_tables(self, dbname):
        """Mostra i nomi delle tabelle all'interno di un database
        
        Args:
            dbname (str): Nome del database di cui vogliamo vedere le tabelle
        """
        self.use_database(dbname)
        self.cursor.execute("SELECT Distinct TABLE_NAME FROM information_schema.TABLES")
        tables = self.cursor.fetchall()
        return [i[0] for i in tables]

    def show_databases(self):
        """Mostra i nomi dei database all'interno del server"""
        self.cursor.execute(
            """select * from sys.databases WHERE name NOT IN('master', 'tempdb', 'model', 'msdb');"""
        )
        databases = self.cursor.fetchall()
        return [i[0] for i in databases]

    def insert_one(self, table_name, values, dbname=None):
        """Inserisce una nuova riga all'interno di una tabella 
        
        Args:
            dbname (str): Nome del database contenente la tabella a cui si vuole aggiugnere la riga
            table_name (str): Nome della tabella a cui si vuole aggiungere la riga
            values (dict): dizionario contenente i valori che compongono la nuova riga che si desidera aggiungere
            fields (list, optional): colonne a cui vengono associati i nuovi valori. Defaults to None.
        """

        logger.debug("insert_one {0}.{1}".format(dbname, table_name))

        if dbname:
            self.use_database(dbname)

        statement = ""
        try:

            fields, values = self._make_list_of_values(values)

            statement = self.insert_statement.format(table_name, fields, values)
            self.cursor.execute(statement)

        except:
            logger.error("insert_one statement: {0}".format(statement or None))
            logger.exception("")
            return 1
        return 0

    def bulk_insertion(
        self,
        list_of_columns,
        data_as_dict,
        dbname,
        table_name,
        commit_every=5000,
        record_each_statement=200,
    ):

        logger.debug("bulk_insertion {0}.{1}".format(dbname, table_name))

        self.use_database(dbname)

        len_data = len(data_as_dict)
        executions = 0

        try:
            multiple_values = []
            for idx, row in enumerate(data_as_dict):

                _, values = self._make_list_of_values(
                    row, list_of_columns=list_of_columns
                )

                multiple_values.append("(" + values + ")")

                if idx > 0 and idx % record_each_statement == 0:

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
                        logger.error("Errore a idx {0}".format(idx))
                        logger.error("Statement {}".format(statement))
                        logger.exception("")
                        return 1

                if idx > 0 and idx % commit_every == 0:
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

    def insert_many(self, data, dbname, table_name, step=5000):
        """Inserisce nuove righe all'interno di una tabella
        
        Args:
            data (list): lista di dizionari, ciascuno di essi corrisponde a una nuova riga.
            dbname (str): Nome della tabella a cui si vuole aggiungere le righe
            table_name (str): Nome della tabella a cui si vuole aggiungere la righe
            step (int, optional): utile per definire nella log quante righe sono già state aggiunte.
                Defaults to 5000.

        .. todo::
            Allo stato attuale esegue una mera iterazione, eseguendo tanti insert statment
            quanti sono i record in input. Occorre implementare bulk insertion basati su
            un singolo statement ogni 'n' records.
        """

        logger.debug("insert_many {0}.{1}".format(dbname, table_name))

        self.use_database(dbname)

        len_data = len(data)
        errors = 0

        try:
            for idx, row in enumerate(data):

                if idx > 0 and idx % step == 0:
                    logger.info("Arrivato a {0}/{1}".format(idx, len_data))
                    self.connection.commit()

                list_of_columns, list_of_values = self._make_list_of_values(row)

                statement = "NONE"
                try:
                    statement = self.insert_statement.format(
                        table_name, list_of_columns, list_of_values
                    )
                    self.cursor.execute(statement)
                except:
                    logger.error("Errore a idx {0}".format(idx))
                    logger.error("Statement {}".format(statement))
                    logger.exception("")
                    errors += 1
                    if errors > 5:
                        return 1
                    continue

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

    def _sql_clean(self, value):
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

    def _make_list_of_values(
        self, values_dict, list_of_columns=None, missing_value=None
    ):

        """Costruisce la lista dei campi e dei valori da inserire

        Dato un dizionario, costruisce due stringhe, contenenti i nomi dei campo
        e i nomi dei valori da inserire all'interno del database. In ciascuna stringa
        i singoli nomi sono separati da una virgola, come atteso dalla sintassi SQL.

        Args:
            values_dict (dict): il dizionario avente per chiave il nome del campo e per valore
                il valore corrispondente

        Returns:
            columns (str): la stringa data dalla concatenazione dei nomi dei campi da inserire
            values (str): la stringa data dalla concatenazione dei valore da inserire
        """

        if not list_of_columns:
            list_of_columns = values_dict.keys()

        values = ""
        columns = ""
        for key in list_of_columns:
            values += self._sql_clean(values_dict.get(key, missing_value))
            columns += ", "
            columns += key

        values = values.strip(",")
        columns = columns.strip(",")

        return columns, values
